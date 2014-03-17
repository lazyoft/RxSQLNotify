using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;

namespace RxSQLNotify
{
	public static class RxSql
	{
		public static IObservable<Unit> Observe(this SqlConnection connection, string statement)
		{
			if((connection.State & ConnectionState.Open) == 0)
				connection.Open();

			return new RepeatableNotification(new SqlCommand(statement, connection))
				.Do(sqlCommand => sqlCommand.ExecuteScalar())
				.Select(_ => Unit.Default);
		}

		class RepeatableNotification : IObservable<SqlCommand>
		{
			readonly ISubject<SqlCommand> Notifications;
			readonly string ConnectionString;
			bool _dependencyStarted;

			void StartSqlDependency()
			{
				if (_dependencyStarted)
					return;

				var principal = Thread.CurrentPrincipal;
				Thread.CurrentPrincipal = null;

				try
				{
					SqlDependency.Start(ConnectionString);
					_dependencyStarted = true;
				}
				catch (Exception ex)
				{
					Trace.TraceError("Exception occurred while registering SQL Dependency: {0}", ex);
					throw;
				}
				finally
				{
					Thread.CurrentPrincipal = principal;
				}
			}

			public RepeatableNotification(SqlCommand template)
			{
				ConnectionString = template.Connection.ConnectionString;
				StartSqlDependency();
				Notifications = new Subject<SqlCommand>();
				Notifications.Subscribe(SubscribeChange);
				SubscribeChange(template);
			}

			void SubscribeChange(SqlCommand template)
			{
				var notification = new SqlDependency(template);
				OnChangeEventHandler onChange = null;
				onChange = (sender, args) =>
				{
					((SqlDependency)sender).OnChange -= onChange;
					if (args.Type == SqlNotificationType.Subscribe)
						Notifications.OnError(new ArgumentException("The command is not compliant with the notifications system or the database is not enabled for notifications."));
					else
						Notifications.OnNext(template.Clone());
				};
				notification.OnChange += onChange;
				template.ExecuteScalar();
			}

			public IDisposable Subscribe(IObserver<SqlCommand> observer)
			{
				return new CompositeDisposable(Notifications.SubscribeSafe(observer), Disposable.Create(() => SqlDependency.Stop(ConnectionString)));
			}
		}
	}
}

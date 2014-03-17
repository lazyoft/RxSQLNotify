using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Reactive;
using System.Threading;
using Dapper;
using FluentAssertions;
using Machine.Specifications;

namespace RxSQLNotify.Tests
{
	public class SqlConnectionSetup
	{
		const string ConnectionString = @"Data Source=(localdb)\Projects;Initial Catalog=TestNotify;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False";
		protected static SqlConnection Connection;
		protected static IList<Unit> Calls;

		Establish context = () =>
		{
			Connection = new SqlConnection(ConnectionString);
			Calls = new List<Unit>();
		};

		Cleanup after = () => Connection.Execute("truncate table Test");
	}

	[Subject(typeof(SqlConnection))]
	public class When_requesting_a_subscription_for_changes_on_a_table : SqlConnectionSetup
	{
		Because of = () =>
		{
			Connection.Observe("select Id, Name, BigText from dbo.Test").Subscribe(Calls.Add);
			Connection.Execute("insert into Test (Name, BigText) values ('Value1', 'BigText1')");
			Thread.Sleep(500);
		};

		It should_receive_a_notification_whenever_the_table_changes = () => Calls.Count.Should().Be(1);
	}

	[Subject(typeof(SqlConnection))]
	public class When_requesting_a_subscription_for_changes_followed_by_subsequent_changes: SqlConnectionSetup
	{
		Because of = () =>
		{
			Connection.Observe("select Id, Name, BigText from dbo.Test").Subscribe(Calls.Add);
			Connection.Execute("insert into Test (Name, BigText) values ('Value1', 'BigText1')");
			Thread.Sleep(500);
			Connection.Execute("insert into Test (Name, BigText) values ('Value2', 'BigText2')");
			Thread.Sleep(500);
			Connection.Execute("update Test set Name = 'NewValue' where Name = 'Value1'");
			Thread.Sleep(500);
		};

		It should_receive_a_notification_for_every_change = () => Calls.Count.Should().Be(3);
	}

	[Subject(typeof(SqlConnection))]
	public class When_requesting_a_subscription_for_changes_on_a_specific_column : SqlConnectionSetup
	{
		Because of = () =>
		{
			Connection.Observe("select BigText from dbo.Test").Subscribe(Calls.Add);
			Connection.Execute("insert into Test (Name, BigText) values ('Value1', 'BigText1')");
			Thread.Sleep(500);
			Connection.Execute("update Test set Name = 'NewValue' where Name = 'Value1'");
			Thread.Sleep(500);
			Connection.Execute("update Test set BigText = 'NewValue' where Name = 'NewValue'");
			Thread.Sleep(500);
		};

		It should_notify_for_the_requested_fields_only = () => Calls.Count.Should().Be(2);
	}

	[Subject(typeof(SqlConnection))]
	public class When_requesting_a_subscription_for_a_table_containing_data : SqlConnectionSetup
	{
		Because of = () =>
		{
			Connection.Execute("insert into Test (Name, BigText) values ('Value1', 'BigText1')");
			Connection.Execute("insert into Test (Name, BigText) values ('Value2', 'BigText2')");
			Connection.Execute("insert into Test (Name, BigText) values ('Value3', 'BigText3')");
			Connection.Execute("insert into Test (Name, BigText) values ('Value4', 'BigText4')");
			Connection.Observe("select BigText from dbo.Test").Subscribe(Calls.Add);
			Connection.Execute("delete from Test where Name = 'Value3'");
			Thread.Sleep(500);
		};

		It should_notify_of_deleted_records_too = () => Calls.Count.Should().Be(1);
	}
}

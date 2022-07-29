using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace Data.Sql.Provider
{
    public class SqlServerProvider : ISqlProvider
    {
        public string ConnectionString { get; set; } = string.Empty;

        public string ProviderType => "System.Data.SqlClient";

        public SqlServerProvider()
        {

        }

        public SqlServerProvider(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public DbConnection CreateConnection()
        {
            return new SqlConnection(ConnectionString);
        }

        public DbConnection CreateOpenedConnection()
        {
            DbConnection connection = new SqlConnection(ConnectionString);
            connection.Open();
            return connection;
        }

        public DbCommand CreateCommand(string commandString, CommandType commandType = CommandType.Text, DbParameter[]? inputParams = null, DbParameter[]? outputParams = null)
        {
            SqlCommand cmd = new(commandString) { CommandType = commandType };
            if (inputParams  is { Length: > 0 }) cmd.Parameters.AddRange(inputParams);
            if (outputParams is { Length: > 0 }) cmd.Parameters.AddRange(outputParams);
            return cmd;
        }

        public DbDataReader CreateReader(IDbCommand command)
        {
            return (DbDataReader)command.ExecuteReader();
        }

        public DbDataReader CreateReader(IDbCommand command, CommandBehavior behavior)
        {
            return (DbDataReader)command.ExecuteReader(behavior);
        }

        public DbParameter CreateInputParameter(string parameterName, object? value, DbType dbType = DbType.Object)
        {
            return new SqlParameter
            {
                ParameterName = parameterName,
                Value = value ?? DBNull.Value,
                Direction = ParameterDirection.Input,
                DbType = dbType
            };
        }

        public DbParameter CreateInputParameter(InParameterInfo inParameterInfo)
        {
            return new SqlParameter
            {
                ParameterName = inParameterInfo.Name,
                Value = inParameterInfo.Value ?? DBNull.Value,
                Direction = ParameterDirection.Input,
                DbType = inParameterInfo.DbType
            };
        }

        public DbParameter CreateOutputParameter(string parameterName)
        {
            return new SqlParameter
            {
                ParameterName = parameterName,
                Direction = ParameterDirection.Output
            };
        }

        public DbParameter CreateReturnParameter()
        {
            return new SqlParameter
            {
                Direction = ParameterDirection.ReturnValue
            };
        }

        public DbParameter[] CreateInputParameters(object source, string parameterPrefix)
        {
            if (source == null) return Array.Empty<DbParameter>();

            PropertyInfo[] properties = source.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            return (from property in properties
                    let parameterInfo = property.PropertyType != typeof(InParameterInfo) ? null : property.GetValue(source) as InParameterInfo
                    select parameterInfo == null ?
                    new SqlParameter(
                        parameterName: parameterPrefix + property.Name,
                        value: property.GetValue(source) ?? DBNull.Value)
                    {
                        Direction = ParameterDirection.Input
                    } :
                    new SqlParameter(
                        parameterName: parameterPrefix + parameterInfo.Name,
                        value: parameterInfo.Value ?? DBNull.Value)
                    {
                        Direction = ParameterDirection.Input,
                        DbType = parameterInfo.DbType
                    }).ToArray();
        }

        public DbParameter[] CreateOutputParameters(string[] source)
        {
            if (source == null) return Array.Empty<DbParameter>();

            return (from parameterName in source
                    select new SqlParameter
                    {
                        ParameterName = parameterName,
                        Direction = ParameterDirection.Output
                    }).ToArray();
        }
    }
}

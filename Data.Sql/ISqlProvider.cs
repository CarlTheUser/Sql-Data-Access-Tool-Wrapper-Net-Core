using System.Data;
using System.Data.Common;

namespace Data.Sql
{
    public interface ISqlProvider
    {
        string ConnectionString { get; set; }

        DbConnection CreateConnection();
        DbConnection CreateOpenedConnection();

        DbCommand CreateCommand(string commandString, CommandType commandType = CommandType.Text, DbParameter[] inputParams = null, DbParameter[] outputParams = null);

        DbDataReader CreateReader(IDbCommand command);
        DbDataReader CreateReader(IDbCommand command, CommandBehavior behavior);

        DbParameter CreateInputParameter(string parameterName, object value, DbType dbType = DbType.Object);
        DbParameter CreateInputParameter(InParameterInfo inParameterInfo);
        DbParameter CreateOutputParameter(string parameterName);
        DbParameter CreateReturnParameter();
        DbParameter[] CreateInputParameters(object source, string parameterPrefix);
        DbParameter[] CreateOutputParameters(string[] source);
    }
}

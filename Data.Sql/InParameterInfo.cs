using System.Data;

namespace Data.Sql
{
    public class InParameterInfo
    {
        public string Name { get; }
        public object Value { get; }
        public DbType DbType { get; }

        public InParameterInfo(string name, object value, DbType dbType)
        {
            Name = name;
            Value = value;
            DbType = dbType;
        }
    }
}

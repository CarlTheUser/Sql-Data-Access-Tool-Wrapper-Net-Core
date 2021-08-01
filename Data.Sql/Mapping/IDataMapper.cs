using System.Data;

namespace Data.Sql.Mapping
{
    public interface IDataMapper<T> where T : class, new()
    {
        T CreateMappedInstance(IDataReader reader);
    }
}

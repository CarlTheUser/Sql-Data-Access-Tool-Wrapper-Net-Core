using Data.Sql.Querying;
using System.Collections.Generic;

namespace Data.Sql
{
    public interface IDao<T>
    {
        void InsertItem(T row, ISqlProvider provider, SqlTransaction transaction);
        void UpdateItem(T row, ISqlProvider provider, SqlTransaction transaction);
        void DeleteItem(T row, ISqlProvider provider, SqlTransaction transaction);
        IEnumerable<T> Find(QueryFilter filter, ISqlProvider provider, SqlTransaction transaction);
    }
}

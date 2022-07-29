using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Data.Sql.Querying
{
    public abstract class SqlQuery<T>
    {
        protected QueryFilter? _filter = null;

        public SqlQuery<T> Filter(QueryFilter filter)
        {
            _filter = filter;

            return this;
        }

        public abstract IEnumerable<T> Execute();
    }

    public abstract class AsyncSqlQuery<T>
    {
        protected QueryFilter? _filter = null;

        public AsyncSqlQuery<T> Filter(QueryFilter filter)
        {
            _filter = filter;

            return this;
        }

        public abstract Task<IEnumerable<T>> ExecuteAsync(CancellationToken cancellationToken);
    }
}

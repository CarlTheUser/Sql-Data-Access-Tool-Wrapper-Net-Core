using System.Collections.Generic;

namespace Data.Sql.Querying
{
    public abstract class SqlQuery<T>
    {
        protected QueryFilter _filter = null;

        public SqlQuery<T> Filter(QueryFilter filter)
        {
            _filter = filter;

            return this;
        }

        public abstract IEnumerable<T> Execute();
    }
}

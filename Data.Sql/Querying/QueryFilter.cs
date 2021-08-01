using System.Data.Common;
using System.Linq;

namespace Data.Sql.Querying
{

    public abstract class QueryFilter
    {
        public virtual DbParameter[] GetParameters() => null;

        public bool UsesParameter { get; protected set; }

        public abstract string ToSqlClause();

        public QueryFilter And(QueryFilter filterCriterion)
        {
            return new CompoundQueryFilter(this, filterCriterion, CompoundQueryFilter.LINK_AND);
        }

        public QueryFilter Or(QueryFilter filterCriterion)
        {
            return new CompoundQueryFilter(this, filterCriterion, CompoundQueryFilter.LINK_OR);
        }

        public QueryFilter CreateBrackets() => new ParenthesizedQueryFilter(this);

    }

    internal class CompoundQueryFilter : QueryFilter
    {
        internal static readonly string LINK_AND = "AND";

        internal static readonly string LINK_OR = "OR";

        private QueryFilter LeftFilter { get; }

        private QueryFilter RightFilter { get; }

        string Link { get; }

        public CompoundQueryFilter(QueryFilter left, QueryFilter right, string link)
        {
            LeftFilter = left;
            RightFilter = right;
            Link = link;
            UsesParameter = LeftFilter.UsesParameter || RightFilter.UsesParameter;
        }

        public override string ToSqlClause()
        {
            return $"{LeftFilter.ToSqlClause()} {Link} {RightFilter.ToSqlClause()}";
        }

        public override DbParameter[] GetParameters()
        {
            if (LeftFilter.UsesParameter && RightFilter.UsesParameter)
                return LeftFilter.GetParameters().Concat(RightFilter.GetParameters()).ToArray();
            else if (LeftFilter.UsesParameter) return LeftFilter.GetParameters();
            else if (RightFilter.UsesParameter) return RightFilter.GetParameters();

            return null;
        }
    }

    internal class ParenthesizedQueryFilter : QueryFilter
    {

        private QueryFilter GroupedCriterion { get; }

        public ParenthesizedQueryFilter(QueryFilter groupedCriterion)
        {
            GroupedCriterion = groupedCriterion;
            UsesParameter = GroupedCriterion.UsesParameter;
        }

        public override DbParameter[] GetParameters()
        {
            return GroupedCriterion.GetParameters();
        }

        public override string ToSqlClause()
        {
            return $"({GroupedCriterion.ToSqlClause()}) ";
        }
    }
}

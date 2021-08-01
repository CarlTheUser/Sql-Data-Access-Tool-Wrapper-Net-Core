using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;

namespace Data.Sql.Mapping
{
    public class DynamicMapper : IDataMapper<ExpandoObject>
    {
        private class ReaderColumnMap
        {
            public string Column { get; }
            public Type Type { get; }

            public ReaderColumnMap(string column, Type type)
            {
                Column = column;
                Type = type;
            }
        }

        private IDictionary<int, ReaderColumnMap> propertyMappingsCache;

        public DynamicMapper(IDataReader schemaSource)
        {
            propertyMappingsCache = CreateMappings(schemaSource);
        }

        private IDictionary<int, ReaderColumnMap> CreateMappings(IDataReader reader)
        {
            IDictionary<int, ReaderColumnMap> propertyMappings = new Dictionary<int, ReaderColumnMap>();

            //The schema of sql table involves the column name and data type for each columns.
            DataTable schema = reader.GetSchemaTable();

            DataColumnCollection dataColumns = schema.Columns;

            //Gets the column name ordinal and data type ordinal

            int nameColumn = dataColumns["ColumnName"].Ordinal;

            int typeColumn = dataColumns["DataType"].Ordinal;

            int ordinalColumn = dataColumns["ColumnOrdinal"].Ordinal;

            DataRowCollection schemaRows = schema.Rows;

            int schemaRowCount = schemaRows.Count - 1;

            for (int i = schemaRowCount; i != -1; --i)
            {
                DataRow row = schemaRows[i];
                string name = row[nameColumn].ToString();
                Type type = Type.GetType(row[typeColumn].ToString());
                int ordinal = Convert.ToInt32(row[ordinalColumn]);
                propertyMappings.Add(ordinal, new ReaderColumnMap(name, type));
            }

            return propertyMappings;
        }

        public ExpandoObject CreateMappedInstance(IDataReader reader)
        {
            IDictionary<int, ReaderColumnMap> cache = propertyMappingsCache;

            var instance = new ExpandoObject();

            int count = propertyMappingsCache.Count - 1;

            foreach (var prop in cache)
            {
                int columnOrdial = prop.Key;
                ReaderColumnMap mapping = prop.Value;
                (instance as IDictionary<string, object>).Add(mapping.Column, Convert.ChangeType(reader[columnOrdial], mapping.Type));
            }

            return instance;
        }
    }
}

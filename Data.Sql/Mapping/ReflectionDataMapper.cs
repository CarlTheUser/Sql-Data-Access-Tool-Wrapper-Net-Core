using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Data.Sql.Mapping
{
    public class ReflectionDataMapper<T> : IDataMapper<T> where T : class, new()
    {
        //private static readonly IDictionary<string, PropertyInfo> PropertyMappingsCache;
        private static readonly Dictionary<string, PropertyMap> PropertyMappingsCache;

        static ReflectionDataMapper()
        {
            if (PropertyMappingsCache == null)
            {
                //PropertyMappingsCache = new Dictionary<string, PropertyInfo>();

                //PropertyMappingsCache = new Dictionary<string, PropertyMap>();

                StringComparer comparer = StringComparer.OrdinalIgnoreCase;

                PropertyMappingsCache = new Dictionary<string, PropertyMap>(comparer);

                Type type = typeof(T);

                PropertyInfo[] properties = type.GetProperties();

                foreach (PropertyInfo prop in properties)
                {
                    var attributes = prop.GetCustomAttributes(false);

                    DataFieldAttribute columnMapping = (DataFieldAttribute)attributes.FirstOrDefault(a => a.GetType() == typeof(DataFieldAttribute));

                    if (columnMapping == null)
                    {
                        PropertyMappingsCache.Add(prop.Name, new PropertyMap(prop));
                    }
                    else
                    {
                        string columnName = columnMapping.Column;

                        PropertyMappingsCache.Add(columnName, new PropertyMap(prop, columnName, columnMapping.FieldMapperType));
                    }
                }
            }
        }

        public virtual T CreateMappedInstance(IDataReader reader)
        {
            IReadOnlyDictionary<string, PropertyMap> mappingsCopy = PropertyMappingsCache;

            int readerColumnCount = reader.FieldCount;

            T item = new T();

            for (int i = 0; i < readerColumnCount; ++i)
            {
                if (mappingsCopy.TryGetValue(reader.GetName(i), out PropertyMap propertyMap))
                {
                    object value = reader[i];
                    if (value != DBNull.Value) propertyMap.PropertyInfo.SetValue(item, !propertyMap.HasCustomMapping ? value : propertyMap.CustomMapping.Map(value), null);
                }
            }

            return item;
        }

        private class PropertyMap
        {
            public PropertyInfo PropertyInfo { get; set; }
            public string Column { get; set; }
            public FieldMapping CustomMapping { get; }

            public bool HasCustomMapping { get; }

            public PropertyMap(PropertyInfo prop) : this(prop, prop.Name) { }

            public PropertyMap(PropertyInfo prop, string column) : this(prop, column, null) { }

            public PropertyMap(PropertyInfo prop, string column, Type customMapperType)
            {
                PropertyInfo = prop;
                Column = column;
                HasCustomMapping = (CustomMapping = customMapperType != null ? (FieldMapping)Activator.CreateInstance(customMapperType) : null) != null;
            }

        }
    }
}

using System;

namespace Data.Sql.Mapping
{
    public class DataFieldAttribute : Attribute
    {
        public string Column { get; }
        //public Func<object, object> CustomMapping { get; } //C# doesnt allow function pointers as constructor parameter for Attribute https://social.msdn.microsoft.com/Forums/vstudio/en-US/0af37f2c-9dbe-48c0-957d-cf6d7223c6bc/valid-custom-attribute-parameter-type?forum=csharpgeneral

        public Type FieldMapperType { get; }

        public DataFieldAttribute(string column, Type fieldMapperType = null)
        {
            Column = column;
            //CustomMapping = customMapping;
            //if (fieldMapperType != null && typeof(FieldMapping).IsAssignableFrom(fieldMapperType)) FieldMapping = (FieldMapping)Activator.CreateInstance(fieldMapperType);
            if (fieldMapperType != null)
            {
                if (typeof(FieldMapping).IsAssignableFrom(fieldMapperType)) FieldMapperType = fieldMapperType;
                else throw new ArgumentException($"Type {nameof(fieldMapperType)} argument must be a Type that inherits from FieldMapping class and has parameterless constructor.");
            }
        }
    }
}

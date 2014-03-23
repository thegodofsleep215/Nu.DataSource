using System;

namespace Nu.DataSource.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class TableReferenceAttribute :Attribute
    {
        public readonly Type Table;
        
        /// <summary>
        /// Set this for many to many.
        /// </summary>
        public Type RelationTable { get; set; }

        /// <summary>
        /// Set this to relate a typed field directly to an entry in another table.
        /// </summary>
        public string ForeignKey { get; set; }

        /// <summary>
        /// Column on the current table that relates to ForeignKey
        /// </summary>
        public string Column { get; set; }

        public TableReferenceAttribute(Type table, string foreignKey, string column)
        {
            Table = table;
            Column = column;
            ForeignKey = foreignKey;
        }
    }
}
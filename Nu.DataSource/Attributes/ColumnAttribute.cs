using System;

namespace Nu.DataSource.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class ColumnAttribute : Attribute
    {
        public bool IsIdent { get; set; }

        public bool IsPKey { get; set; }

        public Asc AscType { get; set; }

        public bool Autoincrement { get; set; }

        // Values in the database can be null.
        public bool CanBeNull { get; set; }

        /// <summary>
        /// Values in the database are unique.
        /// </summary>
        public bool Unique { get; set; }

        /// <summary>
        /// Corresponding name in the database.
        /// </summary>
        public string DbName { get; set; }

        /// <summary>
        /// Primary key ASC type.
        /// </summary>
        public enum Asc
        {
            Asc,
            Desc,
            None
        }
    }
}
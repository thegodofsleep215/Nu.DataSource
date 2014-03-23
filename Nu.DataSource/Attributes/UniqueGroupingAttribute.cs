using System;

namespace Nu.DataSource.Attributes
{
    public enum OnConflict
    {
        Rollback,
        Abort,
        Fail,
        Ignore,
        Replace
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class UniqueGroupingAttribute : Attribute
    {
        public UniqueGroupingAttribute(string[] columns, OnConflict onConflict)
        {
            Columns = columns;
            OnConflict = onConflict;
        }

        public string[] Columns { get; set; }

        public OnConflict OnConflict { get; set; }

    }
}
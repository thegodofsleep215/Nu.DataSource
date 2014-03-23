﻿using System;

namespace Nu.DataSource.Attributes
{

    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public readonly string TableName;

        public TableAttribute(string tableName) { TableName = tableName; }
    }
}

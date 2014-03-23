using System;

namespace Nu.DataSource.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class DataAttribute : Attribute
    {
        /// <summary>
        /// The column used as the key for selecting into a dictionary.
        /// If left blank the identity column will be used.
        /// </summary>
        public string KeyColumn { get; set; }
    }
}
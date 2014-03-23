using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Nu.DataSource.Attributes;

namespace Nu.DataSource.SQLite
{
    public abstract class SqliteDataSource
    {
        #region Abstract Methods

        /// <summary>
        /// This return the data types managed by the datasource. Order is important!
        /// </summary>
        protected abstract Type[] TypesInDatabase { get; }

        protected abstract string DataSourceFile { get; }

        #endregion

        #region Virtual Methods

        protected virtual SQLiteConnection GetConnection()
        {
            string filename = Environment.CurrentDirectory + "\\" + DataSourceFile;
            var builder = new SQLiteConnectionStringBuilder();

            bool runCreate = false;
            if (!File.Exists(filename))
            {
                builder.Add("New", true);
                runCreate = true;
            }
            builder.DataSource = filename;
            builder.SyncMode = SynchronizationModes.Normal;

            var conn = new SQLiteConnection(builder.ConnectionString);
            conn.Open();

            // Create the database if we just made the file.
            if (runCreate)
            {
                foreach (Type t in TypesInDatabase)
                {
                    conn.Create(t);
                }

                InsertDefaultData();

            }

            conn.ExecuteNonQuery("PRAGMA foreign_keys = ON", null);

            return conn;
        }

        /// <summary>
        /// This method is called whe a database is created to insert default data.
        /// </summary>
        protected virtual void InsertDefaultData()
        {
        }

        #endregion

        #region Basic Queries

        public T SelectItem<T>(long ident) where T : Identity
        {
            using (var conn = GetConnection())
            {
                return conn.IdentitySelect<T>(ident);
            }
        }

        public void InsertItemAndUpdateIdent<T>(T item) where T : Identity
        {
            using (var conn = GetConnection())
            {
                conn.IdentityInsert(item);
            }
        }

        public void UpdateItem<T>(T item) where T : Identity
        {
            using(var conn = GetConnection())
            {
                conn.UpdateObject(item);
            }
        }

        public List<T> SelectTableIntoList<T>()
        {
            using(var conn = GetConnection())
            {
                return conn.SelectIntoList<T>();
            }
        } 

        public Dictionary<long, T> SelectTableIntoDictionary<T>() where T : Identity
        {
            using(var conn = GetConnection())
            {
                return conn.IdentityGetAllIntoDictionary<T>();
            }
        }  

        public void Insert(object item)
        {
            using (var conn = GetConnection())
            {
                conn.Insert(item);
            }
        }

        #endregion

        /// <summary>
        /// Loads all public static [Data()] properties on the classes in TypesInDatabase.
        /// </summary>
        protected void LoadData()
        {
            // Load The Data
            foreach (Type type in TypesInDatabase)
            {
                LoadData(type);
            }

            // Load Table Reference Data
            foreach (Type type in TypesInDatabase)
            {
                if(HasReferentialData(type))
                {
                    LoadReferentialData(type);
                }
            }
        }

        private void LoadData(Type type)
        {
            PropertyInfo pInfo;
            DataAttribute dataAtt;
            if ((pInfo = GetData(type, out dataAtt)) != null)
            {
                if (pInfo.PropertyType.Name.StartsWith("Dictionary"))
                {
                    MethodInfo mInfo = typeof(SqliteDataSource).GetMethod("IdentityGetAllIntoDictionary");
                    Type[] genArgs = pInfo.PropertyType.GetGenericArguments();
                    MethodInfo mGen = mInfo.MakeGenericMethod(genArgs);
                    object value = mGen.Invoke(this, new object[] { dataAtt.KeyColumn ?? "" });
                    pInfo.SetValue(null, value, null);
                }
                else if (pInfo.PropertyType.Name.StartsWith("List"))
                {
                    MethodInfo mInfo = typeof(SqliteDataSource).GetMethod("IdentityGetAllIntoList");
                    MethodInfo mGen = mInfo.MakeGenericMethod(pInfo.PropertyType.GetGenericArguments()[0]);
                    pInfo.SetValue(null, mGen.Invoke(this, new object[] { }), null);
                }
                else
                {
                    throw new Exception("Data must be a dictionary or a list.");
                }
            }

        }

        private void LoadReferentialData(Type type)
        {
            PropertyInfo pInfo;
            DataAttribute dataAtt;
            if ((pInfo = GetData(type, out dataAtt)) != null)
            {
                if (pInfo.GetCustomAttributes(typeof(DataAttribute), false).Count() == 1)
                {
                    object data = pInfo.GetValue(null, null);

                    if (pInfo.PropertyType.Name.StartsWith("Dictionary"))
                    {
                        var minfo = typeof(SqliteDataSource).GetMethod("IterateDictionary");
                        minfo.MakeGenericMethod(pInfo.PropertyType.GetGenericArguments()).Invoke(this, new[] { data, type, this });
                    }
                    else if (pInfo.PropertyType.Name.StartsWith("List"))
                    {
                        var minfo = typeof(SqliteDataSource).GetMethod("IterateList");
                        var mgen = minfo.MakeGenericMethod(pInfo.PropertyType.GetGenericArguments());
                        mgen.Invoke(this, new[] { data, type, this });
                    }
                    else
                    {
                        throw new Exception("Data must be a dictionary or a list.");
                    }
                }
            }
        }

        private bool HasReferentialData(Type type)
        {
            return type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Any(pInfo => pInfo.GetCustomAttributes(typeof (TableReferenceAttribute), false).Count() == 1);
        }

        private PropertyInfo GetForigenKey(Type domestic, Type foreign)
        {
            // Iterate throug all of the table references.
            foreach (PropertyInfo pInfo in foreign.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                object[] att = pInfo.GetCustomAttributes(typeof(ForeignKeyAttribute), false);

                if (!att.Any())
                {
                    continue;
                }

                var fKey = (ForeignKeyAttribute)att[0];

                if (domestic == fKey.ForeignTableName)
                {
                    return pInfo;
                }
            }
            throw new Exception("Cannot related data unless all [ForeignKeys()] are declared.");
        }

        private PropertyInfo GetData(Type type, out DataAttribute dataAtt)
        {
            foreach (PropertyInfo pInfo in type.GetProperties(BindingFlags.Static | BindingFlags.Public))
            {
                
                if (pInfo.GetCustomAttributes(typeof(DataAttribute), false).Count() == 1)
                {
                    dataAtt = (DataAttribute)pInfo.GetCustomAttributes(typeof(DataAttribute), false)[0];
                    return pInfo;
                }
            }
            dataAtt = null;
            return null;
        }

        protected void IterateDictionary<TKey, TValue>(Dictionary<TKey, TValue> data, Type type) where TValue : Identity
        {
            // Iterate through all of the table references.
            foreach (PropertyInfo pInfo in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                object[] att = pInfo.GetCustomAttributes(typeof(TableReferenceAttribute), false);
                if (att.Count() == 1)
                {
                    var tRef = (TableReferenceAttribute)att[0];

                    foreach (TKey ident in data.Keys)
                    {
                        string sident = (data[ident] as Identity).Ident.ToString(CultureInfo.InvariantCulture);
                        object value;
                        // One to many relation.
                        if (tRef.RelationTable == null)
                        {
                            PropertyInfo fKeyInfo = GetForigenKey(type, tRef.Table);
                            // Go through every item in the main data set and poplulate the table reference property.
                            string where = BuildWhereClause(fKeyInfo, sident);
                            value = GetValue(pInfo, where, this);
                        }
                        else
                        {
                            PropertyInfo fKeyToMainInfo = GetForigenKey(type, tRef.RelationTable);
                            PropertyInfo fKeyToSecondInfo = GetForigenKey(tRef.Table, tRef.RelationTable);
                            /*
                             * SELECT * FROM <foreign_table>
                             * INNER JOIN <rel_table> ON <rel_table>.ForeignIdent=<foreign_table>.Ident
                             * WHERE <rel_table>.MainIdent=Ident
                             * */
                            Type foreignType = pInfo.PropertyType.GetGenericArguments()[1];
                            string where = BuildWhereClause(tRef, fKeyToMainInfo, fKeyToSecondInfo, foreignType, ident.ToString());
                            value = GetValue(pInfo, where, this);
                        }
                        pInfo.SetValue(data[ident], value, null);
                    }
                }
            }
        }

        protected void IterateList<TValue>(List<TValue> data, Type type) where TValue : Identity
        {
            // Iterate throug all of the table references.
            foreach (PropertyInfo pInfo in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                object[] att = pInfo.GetCustomAttributes(typeof(TableReferenceAttribute), false);
                if (att.Count() == 1)
                {
                    var tRef = (TableReferenceAttribute)att[0];

                    foreach (TValue item in data)
                    {
                        long ident = (item as Identity).Ident;
                        object value;
                        // One to many relation.
                        if (tRef.RelationTable == null)
                        {
                            PropertyInfo fKeyInfo = GetForigenKey(type, tRef.Table);
                            // Go through every item in the main data set and poplulate the table reference property.
                            string where = BuildWhereClause(fKeyInfo, ident.ToString(CultureInfo.InvariantCulture));
                            value = GetValue(pInfo, where, this);
                        }
                        else
                        {
                            PropertyInfo fKeyToMainInfo = GetForigenKey(type, tRef.Table);
                            PropertyInfo fKeyToSecondInfo = GetForigenKey(tRef.Table, tRef.RelationTable);
                            /*
                             * SELECT * FROM <foreign_table>
                             * INNER JOIN <rel_table> ON <rel_table>.ForeignIdent=<foreign_table>.Ident
                             * WHERE <rel_table>.MainIdent=Ident
                             * */
                            Type foreignType = pInfo.PropertyType.GetGenericArguments()[1];
                            string where = BuildWhereClause(tRef, fKeyToMainInfo, fKeyToSecondInfo, foreignType, ident.ToString(CultureInfo.InvariantCulture));
                            value = GetValue(pInfo, where, this);
                        }
                        pInfo.SetValue(data, value, null);
                    }
                }
            }
        }

        private object GetValue(PropertyInfo pInfo, string where, SqliteDataSource source)
        {
            object value;
            if (pInfo.PropertyType.Name.StartsWith("Dictionary"))
            {
                value = typeof(SqliteDataSource).GetMethod("IdentitySelectIntoDictionary").MakeGenericMethod(pInfo.PropertyType.GetGenericArguments()[1]).Invoke(source, new object[] { where });
            }
            else if (pInfo.PropertyType.Name.StartsWith("List"))
            {
                value = typeof(SqliteDataSource).GetMethod("SelectItemIntoList").MakeGenericMethod(pInfo.PropertyType.GetGenericArguments()).Invoke(source, new object[] { where });
            }
            else
            {
                throw new Exception("Data can only be a dictionary or a list.");
            }
            return value;
        }

        private string BuildWhereClause(PropertyInfo fKey, string ident)
        {
            return string.Format(" WHERE {0}={1}", fKey.Name, ident);
        }

        private string BuildWhereClause(TableReferenceAttribute tRef, PropertyInfo fKeyToMainInfo, PropertyInfo fKeyToSecondInfo, Type foreignType, string ident)
        {
            string relTable = ExtensionUtilities.GetTableName(tRef.RelationTable);
            string foreignTable = ExtensionUtilities.GetTableName(foreignType);
            return string.Format(" INNER JOIN {0} ON {0}.{1}={2}.Ident WHERE {0}.{3}={4}", relTable, fKeyToSecondInfo.Name, foreignTable, fKeyToMainInfo.Name, ident);
        }
    }
}

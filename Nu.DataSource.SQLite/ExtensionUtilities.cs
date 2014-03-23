using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Nu.DataSource.Attributes;

namespace Nu.DataSource.SQLite
{
    internal class ExtensionUtilities
    {
        internal static string GetTableName(object obj)
        {
            return GetTableName(obj.GetType());
        }

        internal static string GetTableName(Type t)
        {
            object[] attributes = t.GetCustomAttributes(typeof(TableAttribute), false);

            if (!(attributes.Any()))
                throw new Exception(String.Format("Found an invalid number ({0}) of 'Table attribute(s).", attributes.Count()));

            return ((TableAttribute)attributes[0]).TableName;
        }

        internal static string BuildSelectQuery(Type type)
        {
            return String.Format("SELECT * FROM {0}", GetTableName(type));
        }

        internal static string BuildInsertQuery(Object obj, out Dictionary<string, object> paramDict)
        {
            string columns, parameters, tableName;
            GetInsertGuts(obj, out paramDict, out parameters, out columns, out tableName);
            return String.Format("INSERT INTO {0} ({1}) values ({2})", tableName, columns, parameters);
        }

        internal static string BuildInsertOrReplaceQuery(Object obj, out Dictionary<string, object> paramDict)
        {
            string columns, parameters, tableName;
            GetInsertGuts(obj, out paramDict, out parameters, out columns, out tableName);
            return String.Format("INSERT OR REPLACE INTO {0} ({1}) values ({2})", tableName, columns, parameters);
        }

        internal static void GetInsertGuts(Object obj, out Dictionary<string, object> paramDict, out string parameters,
            out string columns, out string tableName)
        {
            int count = 0;
            paramDict = new Dictionary<string, object>();

            tableName = GetTableName(obj);

            parameters = "";
            columns = "";

            foreach (PropertyInfo info in obj.GetType().GetProperties())
            {
                object[] attributes = info.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (!attributes.Any())
                    continue;

                // If there are more, oh well whom ever made it is dumb.
                var c = (ColumnAttribute)attributes[0];

                // Ignore the identity field.
                if (c.IsIdent)
                {
                    object ident = info.GetValue(obj, null);
                    if (ident is long)
                    {
                        if ((long)ident == 0)
                        {
                            continue;
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
                var name = string.IsNullOrEmpty(c.DbName) ? info.Name : c.DbName;
                string p = String.Format("@_{0}{1}", name, count);
                paramDict.Add(p, info.GetValue(obj, null));
                parameters += p + ",";
                columns += name + ",";

                count++;
            }
            parameters = parameters.Substring(0, parameters.Length - 1);
            columns = columns.Substring(0, columns.Length - 1);
        }

        internal static string BuildUpdateQuery(Object obj, out Dictionary<string, object> paramDict)
        {
            return BuildUpdateQuery(obj, "", out paramDict);
        }

        internal static string BuildUpdateQuery(Object obj, string whereClause, out Dictionary<string, object> paramDict)
        {
            string setList;
            string tableName;
            GetUpdateGuts(obj, whereClause, out paramDict, out setList, out tableName, out whereClause);

            if (whereClause.Length == 0)
                throw new Exception("Error building Insert query from object. Either no primary key was specified, or no where clause was given.");

            return String.Format("UPDATE {0} SET {1} WHERE {2}", tableName, setList, whereClause);
        }

        internal static void GetUpdateGuts(Object obj, string whereClause, out Dictionary<string, object> paramDict, out string setList, out string tableName, out string newWhereClause)
        {
            paramDict = new Dictionary<string, object>();
            setList = "";
            newWhereClause = whereClause ?? "";

            tableName = GetTableName(obj);

            int count = 0;
            foreach (PropertyInfo info in obj.GetType().GetProperties())
            {
                object[] attributes = info.GetCustomAttributes(typeof(ColumnAttribute), true);
                if (!attributes.Any())
                    continue;

                var c = (ColumnAttribute)attributes[0];

                var name = string.IsNullOrEmpty(c.DbName) ? info.Name : c.DbName;
                string p = String.Format("@_{0}{1}", name, count);
                object value = info.GetValue(obj, null);
                paramDict.Add(p, value);

                if (c.IsPKey)
                {
                    if (newWhereClause.Length > 0)
                        newWhereClause += " AND ";
                    newWhereClause += String.Format("{0}={1}", name, p);
                }
                else
                {
                    setList += String.Format("{0}={1},", name, p);
                }

                count++;
            }
            setList = setList.Substring(0, setList.Length - 1);
        }

        internal static string BuildDeleteQuery(Object obj, out Dictionary<string, object> paramDict)
        {
            return BuildDeleteQuery(obj, "", out paramDict);
        }

        internal static string BuildDeleteQuery(Object obj, string whereClause, out Dictionary<string, object> paramDict)
        {
            string tableName;
            GetDeleteGuts(obj, whereClause, out paramDict, out tableName, out whereClause);

            if (whereClause.Length == 0)
                throw new Exception("Error building Delete query from object. Either no primary key was specified, or no where clause was given.");

            return String.Format("DELETE FROM {0} WHERE {1}", tableName, whereClause);
        }

        internal static void GetDeleteGuts(Object obj, string whereClause, out Dictionary<string, object> paramDict, out string tableName, out string newWhereClause)
        {
            paramDict = new Dictionary<string, object>();
            newWhereClause = whereClause ?? "";

            tableName = GetTableName(obj);

            int count = 0;
            foreach (PropertyInfo info in obj.GetType().GetProperties())
            {
                object[] attributes = info.GetCustomAttributes(typeof(ColumnAttribute), true);
                if (!attributes.Any())
                    continue;

                var c = (ColumnAttribute)attributes[0];

                if (!c.IsPKey)
                    continue;

                var name = string.IsNullOrEmpty(c.DbName) ? info.Name : c.DbName;
                string p = String.Format("@_{0}{1}", name, count);
                object value = info.GetValue(obj, null);
                paramDict.Add(p, value);

                if (newWhereClause.Length > 0)
                    newWhereClause += " AND ";
                newWhereClause += String.Format("{0}={1}", name, p);

                count++;
            }
        }

        internal static string BuildCreate(Type t)
        {
            string tableName, columns;
            GetCreateGuts(t, out tableName, out columns);
            return  String.Format("CREATE TABLE {0} ({1})", tableName, columns);
        }

        internal static void GetCreateGuts(Type t, out string tableName, out string columns)
        {
            tableName = GetTableName(t);
            columns = "";

            foreach (PropertyInfo info in t.GetProperties())
            {
                object[] attributes = info.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (!attributes.Any())
                    continue;

                // If there are more, oh well whom ever made it is dumb.
                var c = (ColumnAttribute)attributes[0];
                string constraints = "";

                // build constraints
                if (c.IsPKey)
                {
                    constraints += " PRIMARY KEY ";
                    if (c.AscType == ColumnAttribute.Asc.Asc)
                        constraints += " ASC ";
                    else if (c.AscType == ColumnAttribute.Asc.Desc)
                        constraints += " DESC ";

                    if (c.Autoincrement)
                        constraints += " AUTOINCREMENT ";
                }

                if (!c.CanBeNull && !c.Autoincrement)
                    constraints += " NOT NULL ";

                if (c.Unique)
                    constraints += " UNIQUE ";
                string name = string.IsNullOrEmpty(c.DbName) ? info.Name : c.DbName;
                columns += String.Format("{0} {1} {2},", name, GetDbType(info), constraints);
     
            }
            columns = columns.Substring(0, columns.Length - 1);

            // Table Constraints
            string tableConstraints = "";
            foreach (PropertyInfo info in t.GetProperties())
            {
                object[] attributes = info.GetCustomAttributes(typeof(ForeignKeyAttribute), false);
                object[] columAtt = info.GetCustomAttributes(typeof (ColumnAttribute), false);
                if (!attributes.Any() || !columAtt.Any())
                    continue;

                // If there are more, oh well whom ever made it is dumb.
                var fk = (ForeignKeyAttribute)attributes[0];
                var c = (ColumnAttribute) columAtt[0];

                string endClause = fk.EndClause ?? "";

                var name = string.IsNullOrEmpty(c.DbName) ? info.Name : c.DbName;
                tableConstraints += String.Format("FOREIGN KEY ({0}) REFERENCES {1} ({2}) {3},", name, GetTableName(fk.ForeignTableName), fk.ForeignColumnName, endClause);

            }

            var ugs = t.GetCustomAttributes(typeof(UniqueGroupingAttribute), false).Cast<UniqueGroupingAttribute>();
            tableConstraints = ugs.Aggregate(tableConstraints, (current, ug) => current + String.Format("UNIQUE ({0}) ON CONFLICT {1},", string.Join(", ", ug.Columns), ug.OnConflict));


            if (tableConstraints.Length > 0)
            {
                tableConstraints = tableConstraints.TrimEnd(',', ' ');
                columns += "," + tableConstraints;
            }
        }

        private static string GetDbType(PropertyInfo pInfo)
        {
            if (pInfo.PropertyType == typeof(Int32) || pInfo.PropertyType == typeof(long))
            {
                return "INTEGER";
            }
            if (pInfo.PropertyType == typeof(string))
            {
                return "TEXT";
            }
            if (pInfo.PropertyType == typeof(float))
            {
                return "REAL";
            }
            if (pInfo.PropertyType == typeof(bool))
            {
                return "BOOL";
            }
            if (pInfo.PropertyType.IsEnum)
            {
                return "INTEGER";
            }
            if (pInfo.PropertyType == typeof(DateTime) || pInfo.PropertyType == typeof(DateTime?))
            {
                return "TEXT";
            }

            return "BLOB";
        }
   
        internal static T BuildObject<T>(Dictionary<string, object> row)
        {
            return (T) BuildObject(row, typeof (T));
        }

        internal static object BuildObject(Dictionary<string, object> row, Type type )
        {
            var obj = type.GetConstructor(new Type[] {});
            if(obj == null)
            {
                throw new ArgumentException(string.Format("{0} does not have a public, empty constructor.", type.Name));
            }
            var result = obj.Invoke(new object[]{});
            foreach (string column in row.Keys)
            {
                var info = GetProperty(column, type);
                info.SetValue(result, row[column], null);
            }
            return result;
        }

        internal static PropertyInfo GetProperty<T>(string column)
        {
            return GetProperty(column, typeof (T));
        }

        internal static PropertyInfo GetProperty(string column, Type type)
        {
            var pInfos = type.GetProperties();
            foreach (var info in pInfos)
            {
                var columnAtt = info.GetCustomAttribute<ColumnAttribute>();
                if (columnAtt.DbName == column)
                {
                    return info;
                }

            }
            return type.GetProperty(column);
        }

        internal static KeyValuePair<TKey, TValue> BuildObject<TKey, TValue>(Dictionary<string, object> row, string sKey)
        {
            var result = new KeyValuePair<TKey,TValue>();
            ConstructorInfo ci = typeof(TValue).GetConstructor(new Type[] { });
            if(ci == null)
            {
                throw new ArgumentException(string.Format("{0} does not have a public, empty constructor.", typeof(TValue).Name));
            }
            var value = (TValue)ci.Invoke(new object[] { });

            foreach (string column in row.Keys)
            {
                PropertyInfo info = GetProperty<TValue>(column);
                object itemValue =  row[column];
                if (info.PropertyType == typeof(DateTime?))
                {
                    DateTime time;
                    if (!DateTime.TryParse(itemValue.ToString(), out time))
                    {
                        continue; // Let it be null.
                    }
                    itemValue = time;
                }
                else if (info.PropertyType == typeof(DateTime))
                {
                    DateTime time = DateTime.Parse(itemValue.ToString());
                    itemValue = time;
                }
                info.SetValue(value, itemValue, null);
                if (column == sKey)
                {
                    var key = (TKey)row[column];
                    result = new KeyValuePair<TKey, TValue>(key, value);
                }
            }
            return result;
        }

        internal static string GetPrimaryKey<TKey, TValue>()
        {
            var properties = (from pInfo in typeof (TValue).GetProperties() 
                                  let attributes = pInfo.GetCustomAttributes(typeof (ColumnAttribute), false) 
                              from att in attributes 
                                  let c = (ColumnAttribute) att 
                              where c.IsPKey 
                              select pInfo).ToList();

            if (properties.Count == 1)
            {
                if (properties[0].PropertyType.FullName == typeof(TKey).FullName)
                {
                    return properties[0].Name;
                }
                throw new Exception(string.Format("Primary key type mismatch, {0} != {1}", properties[0].PropertyType.FullName,
                                                  typeof(TKey).FullName));
            }

            throw new Exception(string.Format("{0} did not have exactly one primary key, it had {1}", typeof(TValue).FullName, properties.Count));
        }
    }
}

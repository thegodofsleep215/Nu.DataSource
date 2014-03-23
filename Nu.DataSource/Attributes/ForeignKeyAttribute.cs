using System;

namespace Nu.DataSource.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ForeignKeyAttribute : Attribute
    {
        public readonly Type ForeignTableName;
        public readonly string ForeignColumnName;
        public readonly Actions UpdateAction;
        public readonly Actions DeleteAction;
        /// <summary>
        /// Used for "ON DELETE/UPDATE" or "NOT DEFERRABLE."
        /// </summary>
        public string EndClause
        {
            get
            {
                string temp = "";
                temp += UpdateAction != Actions.NoAction ? " ON UPDATE " + StringifyAction(UpdateAction) + " " : "";
                temp += DeleteAction != Actions.NoAction ? " ON DELETE " + StringifyAction(DeleteAction) : "";
                return temp;
            }

        }

        private string StringifyAction(Actions action)
        {
            switch(action)
            {
                case Actions.NoAction : return "NO ACTION";
                case Actions.Restriction : return "RESTRICTION";
                case Actions.SetNull : return "SET NULL";
                case Actions.SetDefault : return "SET DEFAULT";
                case Actions.Cascade: return "CASCADE";
            }
            return "NO ACTION";
        }

        public ForeignKeyAttribute(Type foreignTable, string foreignColumnName, Actions deleteAction = Actions.NoAction, Actions updateAction = Actions.NoAction)
        { 
            ForeignTableName = foreignTable; 
            ForeignColumnName = foreignColumnName;
            UpdateAction = updateAction;
            DeleteAction = deleteAction;
        }

        public enum Actions
        {
            NoAction,
            Restriction,
            SetNull,
            SetDefault,
            Cascade
        }
    }
}
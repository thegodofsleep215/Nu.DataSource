namespace Nu.DataSource
{
    /// <summary>
    /// This interface is used to enforce where constraints on SqliteDataSource functions.
    /// Use it with classes that have a single identity of the long value type.
    /// </summary>
    public interface Identity
    {
        long Ident { get; set; }
    }
}
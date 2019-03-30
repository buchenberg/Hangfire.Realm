namespace Hangfire.Realm.Models
{
    public interface IKeyValue
    {
        string Key { get; set; }
        string Value { get; set; }
    }
}
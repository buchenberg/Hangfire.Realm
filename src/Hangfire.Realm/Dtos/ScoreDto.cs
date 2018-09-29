using Realms;

namespace Hangfire.Realm.Dtos
{
    public class ScoreDto : RealmObject
    {
        public string Value { get; set; }
        public double Score { get; set; }
    }
}
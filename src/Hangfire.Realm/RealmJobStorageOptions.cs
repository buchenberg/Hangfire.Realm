using System;
using System.Collections.Generic;
using System.Text;
using Realms;

namespace Hangfire.Realm
{
    public class RealmJobStorageOptions
    {
	    public string DatabasePath { get; set; } = $"{Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)}\\Realm";
		internal RealmConfiguration RealmConfiguration { get; set; }
    }
}

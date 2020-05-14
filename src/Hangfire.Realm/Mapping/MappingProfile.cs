using System;
using System.Collections.Generic;
using System.Text;
using AutoMapper;
using Hangfire.Realm.Models;
using Hangfire.Storage;

namespace Hangfire.Realm.Mapping
{
    public class MappingProfile : Profile
    {
        public MappingProfile()
        {
            CreateMap<JobDto, JobData>();
        }
    }
}

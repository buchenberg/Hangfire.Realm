using System;
using Hangfire.Realm.Dtos;
using Hangfire.States;

namespace Hangfire.Realm.Extensions
{
    internal static class StateExtensions
    {
        public static void AddToStateHistory(this JobDto jobDto, IState state)
        {
            var stateData = new StateDto
            {
                Reason = state.Reason,
                Name = state.Name
            };
            foreach (var data in state.SerializeData())
            {
                stateData.Data.Add(new KeyValueDto(data.Key, data.Value));
            }
                
            jobDto.StateHistory.Add(stateData);
        }
    }
}
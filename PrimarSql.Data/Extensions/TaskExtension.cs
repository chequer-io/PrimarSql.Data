using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace PrimarSql.Data.Extensions;

public static class TaskExtension
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WaitSynchronously(this Task task)
    {
        try
        {
            task.Wait();
        }
        catch (AggregateException e) when (e is { InnerExceptions.Count: 1 })
        {
            throw e.InnerExceptions[0];
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T GetResultSynchronously<T>(this Task<T> task)
    {
        try
        {
            return task.Result;
        }
        catch (AggregateException e) when (e is { InnerExceptions.Count: 1 })
        {
            throw e.InnerExceptions[0];
        }
    }
}

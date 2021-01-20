using System;
using System.Collections;
using System.Collections.Generic;

namespace PrimarSql.Data.Models
{
    public class EmptyEnumerable<T> : IEnumerable<T> where T : class
    {
        public int Count { get; }
        
        public EmptyEnumerable(int count)
        {
            Count = count;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new EmptyEnumerator<T>(Count);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class EmptyEnumerator<TType> : IEnumerator<TType> where TType : class
        {
            private int _currentCount = 0;
            
            public int Count { get; }
            
            public EmptyEnumerator(int count)
            {
                Count = count;
            }

            public bool MoveNext()
            {
                if (_currentCount < Count)
                {
                    _currentCount++;
                    return true;
                }

                return false;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public TType Current => null;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Database
{
    public interface IDBRow
    {
        long ID { get; set; }
        
        void AddField(String fieldName, Object fieldValue);
    }
}

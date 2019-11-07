using System;
using System.Diagnostics;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MovieDatabaseWorker
{
    public class MDBW_ErrorHandler
    {

        public bool IsStuck { get; set; }
        public int ConsecutiveErrorCount { get; set; }
        public int ConsecutiveSleeps { get; set; }
        public int ConsecutiveErrorSleepTime { get; set; }
        public int ConsecutiveErrorRestart { get; set; }


        public MDBW_ErrorHandler()
        {
            IsStuck = false;
            ConsecutiveErrorCount = 0;
            ConsecutiveSleeps = 50;
            ConsecutiveErrorSleepTime = 60000;
            ConsecutiveErrorRestart = 100;
        }

        public void IncreaseConsecutvieErrorCount()
        {
            ConsecutiveErrorCount += 1;
            //if (ConsecutiveErrorCount >= ConsecutiveErrorSleep && ConsecutiveErrorCount < ConsecutiveErrorRestart)
            //{
            //    Thread.Sleep(ConsecutiveErrorSleepTime);
            //}

            if (ConsecutiveErrorCount > ConsecutiveErrorRestart)
            {
                RestartApplication();
            }
        }

        public void ResetConsecutvieErrorCount()
        {
            ConsecutiveErrorCount = 0;
        }

        public void RestartApplication()
        {
            // Get file path of current process 
            var filePath = Assembly.GetExecutingAssembly().Location;
            //var filePath = Application.ExecutablePath;  // for WinForms

            // Start program
            Process.Start(filePath);

            // For Windows Forms app
            //Application.Exit();

            // For all Windows application but typically for Console app.
            Environment.Exit(0);
        }

    }
}

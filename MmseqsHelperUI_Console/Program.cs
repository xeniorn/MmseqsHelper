﻿namespace MmseqsHelperUI_Console
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            var a = new MmseqsHelperLib.MmseqsHelper();

            a.TestAsync().Wait();


        }
    }

    


}
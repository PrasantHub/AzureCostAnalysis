using System;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

[assembly: FunctionsStartup(typeof(ProcessCostFile_Timer.Startup))]

namespace ProcessCostFile_Timer
{
    class Startup: FunctionsStartup
    {
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
		// connection to the azure app configuration service            
	    string cs = "Endpoint=https://<app configuration name>.azconfig.io;Id=<secret code>";
            builder.ConfigurationBuilder.AddAzureAppConfiguration(cs);
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
        }
    }
}

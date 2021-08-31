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
            string cs = "Endpoint=https://app-d-config.azconfig.io;Id=uqy5-l9-s0:C66Hw0Ydm/tRnXh2cjYv;Secret=i4FT8XlTGfpo1JjHbQgHS6BN3YQqd5PJsIQOzyIPMiU=";
            builder.ConfigurationBuilder.AddAzureAppConfiguration(cs);
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
        }
    }
}

using CalibrationManager.Services.Extensions;
using EntityList.Services.Extensions;
using FRCManager.Services.Extensions;
using ITT.Captcha.Abstractions;
using ITT.Captcha.Services;
using ITT.DocumentManage.Services.Extensions;
using ITT.Logger.Abstractions;
using ITT.Logger.Services;
using ITT.Rule.Engine.Services.Extensions;
using ITT.ServiceNow.Services.Extentions;
using ITT.SharePoint.Integration.Services.Extensions;
using ITTPortal.Admin.Services.Extensions;
using ITTPortal.BuyersPortal.Services.Extensions;
using ITTPortal.CAPManager.Services.Extensions;
using ITTPortal.CCA.Services.Extensions;
using ITTPortal.CLMManager.Services.Extensions;
using ITTPortal.Common.Abstractions;
using ITTPortal.Common.Services;
using ITTPortal.Common.Services.Extensions;
using ITTPortal.Core.Abstractions;
using ITTPortal.Core.Models;
using ITTPortal.Core.Services;
using ITTPortal.CronJobConfiguration.Services.Extensions;
using ITTPortal.CronJobScheduler.Services.Extensions;
using ITTPortal.Holiday.Services.Extensions;
using ITTPortal.Identity.Interfaces;
using ITTPortal.Identity.Services;
using ITTPortal.Infrastructure;
using ITTPortal.Infrastructure.Repositories;
using ITTPortal.Infrastructure.SievoContext;
using ITTPortal.Locator.Services.Extensions;
using ITTPortal.Onboarding.Services.Extension;
using ITTPortal.PartManager.Services.Extensions;
using ITTPortal.POApprovals.Abstraction;
using ITTPortal.POApprovals.Infrastructure;
using ITTPortal.POApprovals.Services;
using ITTPortal.RMAManager.Services.Extensions;
using ITTPortal.ShareFiles.Services.Extensions;
using ITTPortal.SSA.Services.Extensions;
using ITTPortal.Workday.Services.Extensions;
using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using MyTasks.Abstractions;
using MyTasks.Services;
using MyTasks.Services.Extensions;
using Newtonsoft.Json;
using NLog.ITTPortal.Services.Extensions;
using PPMManager.Services.Extensions;
using ProjectPlan.Services.Extensions;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;


namespace ITTPortal.Web
{
    public class Startup
    {

        private protected readonly string key = "vRZG2vXfcfULw9MyCYMP45CCZ2M6ynzB";
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;


            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", EnvironmentVariableTarget.Machine);

            if (environment?.ToLower() == "prod")
            {
                // AES key must be 16, 24, or 32 bytes long
                string[] regionsToDecrypt = {
            "ServiceNowIntegration:Client_ID",
            "ServiceNowIntegration:Client_Secret",
            "ServiceNowIntegration:Username",
            "ServiceNowIntegration:Password",
            "WBAWorkdayIntegration:Refresh_Token",
            "WBAWorkdayIntegration:Client_ID",
            "WBAWorkdayIntegration:Client_Secret",
            "WorkdayIntegration:Refresh_Token",
            "WorkdayIntegration:Client_ID",
            "WorkdayIntegration:Client_Secret",
        };

                DecryptAppSettings(Configuration, key, regionsToDecrypt);
            }

    

        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            // PO Approvals
            services.AddScoped<IPoApprovalChainBuilder, PoApprovalChainBuilder>();
            services.AddScoped<IPoApprovalOutboxRepository, PoApprovalOutboxRepository>();
            services.AddScoped<IPoApprovalChainRepository, PoApprovalChainRepository>();
            services.AddScoped<IPoApprovalAuditRepository, PoApprovalAuditRepository>();
            services.AddScoped<IPoApprovalNotifier, PoApprovalNotifier>();
            services.AddScoped<IProcessApprovalOutboxJob, ProcessApprovalOutboxJob>();
            services.AddScoped<PoApprovalsService>();
            services.AddScoped<IPrmsWriter, PrmsWriter>();
            services.AddScoped<IPurchaseOrderQueryService, PurchaseOrderQueryService>();
            services.AddScoped<IPrmsAuditInputResolver, PrmsAuditInputResolverFromStage>();

            services.Configure<CookiePolicyOptions>(options =>
            {
                // This lambda determines whether user consent for non-essential cookies is needed for a given request.
                options.CheckConsentNeeded = context => true;
                options.MinimumSameSitePolicy = SameSiteMode.None;
            });


            ConfigureJwt(services);

            services.AddDbContext<PortalDbContext>(options =>
            {
                options.EnableSensitiveDataLogging();
                options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
            });


            services.AddDbContext<SievoDbContext>(options =>
            {
                options.EnableSensitiveDataLogging();
                options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
            });

            try
            { 
                services.AddDbContext<FrcManagerDbContext>(options =>
                {
                    options.EnableSensitiveDataLogging();
                    options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                    options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
                });
             }
            catch(Exception ex)
            {

            }

            try
            {
                services.AddDbContext<OrderManagerDbContext>(options =>
                {
                    options.EnableSensitiveDataLogging();
                    options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                    options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
                });
            }
            catch(Exception ex)
            {

            }

            try
            {
                services.AddDbContextFactory<WorkdayManagerDbContext>(options =>
                {
                    options.EnableSensitiveDataLogging();
                    options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                    options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
                });
            }
            catch (Exception ex)
            {

            }


            try
            {
                services.AddDbContextFactory<DocumentStorageDbContext>(options =>
                {
                    options.EnableSensitiveDataLogging();
                    options.ConfigureWarnings(w => w.Throw(RelationalEventId.MultipleCollectionIncludeWarning));
                    options.UseLoggerFactory(LoggerFactory.Create(builder => builder.AddConsole()));
                });
            }
            catch (Exception ex)
            {

            }

            services.AddAuthorization();

            services.Configure<IISServerOptions>(options =>
            {
                options.MaxRequestBodySize = 200000000;
            });


            services.Configure<FormOptions>(options =>
            {
                // This lambda determines whether user consent for non-essential cookies is needed for a given request.
                options.MultipartBodyLengthLimit = 200000000;
            });

            // register the repositories
            InitializeDI(services);

            services.AddMvc()
                    .AddNewtonsoftJson(options =>
                    {
                        options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                    });

            // Windows Authentication Negatiate. Just for the handshake.
            // The Authentication/Authorization after the first request will continue with JWToken 
            services.AddAuthentication(NegotiateDefaults.AuthenticationScheme).AddNegotiate();

            services.AddAuthorization(options =>
            {
                options.FallbackPolicy = options.DefaultPolicy;
            });

            services.AddHttpContextAccessor();
            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                try
                {
                    c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
                    {
                        Description = @"JWT Authorization header using the Bearer scheme.
                            Enter 'Bearer' [space] and then your token in the text input below.
                            Example: 'Bearer 12345abcdef'",
                        Name = "Authorization",
                        In = ParameterLocation.Header,
                        Type = SecuritySchemeType.ApiKey,
                        Scheme = "Bearer"
                    });

                    c.AddSecurityRequirement(new OpenApiSecurityRequirement()
                  {
                    {
                      new OpenApiSecurityScheme
                      {
                        Reference = new OpenApiReference
                          {
                            Type = ReferenceType.SecurityScheme,
                            Id = "Bearer"
                          },
                          Scheme = "oauth2",
                          Name = "Bearer",
                          In = ParameterLocation.Header,

                        },
                        new List<string>()
                      }
                    });
                }
                catch (Exception)
                {

                }
            });

            // In production, the Angular files will be served from this directory
            services.AddSpaStaticFiles(configuration =>
            {
                configuration.RootPath = "ClientApp/dist";
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            // Register Syncfusion License
            Syncfusion.Licensing.SyncfusionLicenseProvider.RegisterLicense("Ngo9BigBOggjHTQxAR8/V1NNaF5cXmBCf1FpRmJGdld5fUVHYVZUTXxaS00DNHVRdkdmWXdccHRdRGFcWEJ0W0VWYEo=");

            if (env.EnvironmentName == "dev" || env.EnvironmentName == "uat")
            {
                // run migration if new deployment contains changes
                using (var serviceScope = app.ApplicationServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
                {
                    var context = serviceScope.ServiceProvider.GetService<PortalDbContext>();
                    context.Database.Migrate();
                }

                app.UseDeveloperExceptionPage();
            }
            else
            {
                //app.UseExceptionHandler("/error");
                //app.UseStatusCodePagesWithReExecute("/error/{0}");


                app.UseExceptionHandler(errApp =>
                {
                    errApp.Run(async context =>
                    {
                        context.Response.ContentType = "application/json";
                        // default to 500 unless something already set
                        if (context.Response.StatusCode < 400) context.Response.StatusCode = 500;

                        var feature = context.Features.Get<Microsoft.AspNetCore.Diagnostics.IExceptionHandlerPathFeature>();
                        var problem = new
                        {
                            title = "Unhandled server error",
                            status = context.Response.StatusCode,
                            path = feature?.Path,
                            detail = feature?.Error?.Message
                        };
                        await context.Response.WriteAsJsonAsync(problem);
                    });
                });



                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();
            app.UseSpaStaticFiles();

            app.UseRouting();
            app.UseSwagger();
            app.UseSwaggerUI();

            app.UseAuthentication();
            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();

                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller}/{action=Index}/{id?}");
            });

            app.UseSpa(spa =>
            {
                // To learn more about options for serving an Angular SPA from ASP.NET Core,
                // see https://go.microsoft.com/fwlink/?linkid=864501

                spa.Options.SourcePath = "ClientApp";
           
                if (env.IsDevelopment())
                {
                    //spa.UseAngularCliServer(npmScript: "start");
                    spa.UseProxyToSpaDevelopmentServer("http://localhost:4200"); //4200
                }
            });
        }

        public void ConfigureJwt(IServiceCollection services)
        {
            var settings = GetJwtSettings();
            services.AddSingleton<JwtSettings>(settings);

            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = "JwtBearer";
                options.DefaultChallengeScheme = "JwtBearer";
            })
            .AddJwtBearer("JwtBearer", options =>
            {
                options.TokenValidationParameters =
                    new TokenValidationParameters
                    {
                        ValidateIssuerSigningKey = true,
                        IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(settings.Key)),
                        ValidateIssuer = true,
                        ValidIssuer = settings.Issuer,
                        ValidateAudience = true,
                        ValidAudience = settings.Audience,
                        ValidateLifetime = true,
                        ClockSkew = TimeSpan.FromMinutes(settings.MinutesToExpiration)
                    };
            });
        }

        public JwtSettings GetJwtSettings()
        {
            var settings = new JwtSettings
            {
                Key = Configuration["JwtToken:key"],
                Audience = Configuration["JwtToken:audience"],
                Issuer = Configuration["JwtToken:issuer"],
                MinutesToExpiration = Convert.ToInt32(Configuration["JwtToken:minutestoexpiration"])
            };

            return settings;
        }

        private static void InitializeDI(IServiceCollection services)
        {
            services.AddTransient<IHttpContextAccessor, HttpContextAccessor>();
            services.AddTransient<IUserResolverService, UserResolverService>();
            services.AddTransient<IADFSService, ADFSService>();
            services.AddTransient<INotificationService, NotificationService>();

            services.AddTransient<IWindowsUserService, WindowsUserService>();
            services.AddTransient<ICaptchaService, CaptchaService>();
            services.AddTransient<IUserIdentityRepository, UserIdentityRepository>();
            services.AddTransient<IIdentityService, IdentityService>();
            services.AddTransient<IBrowserRepository, BrowserRepository>();

            services.AddSingleton<IMailService, MailService>();
            services.AddTransient<IBrowserService, BrowserService>();
            
            //services.AddSingleton<ISharePointService, SharePointService>();

            services.AddSingleton<ILoggerService, LoggerService>();
            services.RegisterMyTasksModule();

            // Module Registrations
            services.RegisterAdminModule();
            services.RegisterCommonModule();

            services.RegisterHolidayModule();
            services.RegisterOfficeModule();
            services.RegisterVendorModule();
            services.RegisterLogManagerModule();
            services.RegisterRMAManagerModule();
            services.RegisterCAPManagerModule();
            services.RegisterCLMManagerModule();
            services.RegisterRulesModule();
            services.RegisterDocumentManageModule();
            services.RegisterSharePointServiceModule();

            services.RegisterCronJobSchedulerModule();
            services.RegisterCronJobCongiModuleExtensions();

            services.RegisterSSAModule();
            services.RegisterProjectPlanModule();
            services.RegisterPartManagerModule();

            services.RegisterBuyersPortalModule();
            services.RegisterCalibrationManagerModule();
            services.RegisterShareFilesModule();

            services.RegisterPCNModule();
            services.RegisterOnboardingModule();
            services.RegisterWorkdayModule();
            services.RegisterCCAModule();
            services.RegisterPPMModule();

            services.RegisterServiceNowModule();

            services.RegisterFRCModule();
            services.RegisterEntityListModule();
            //services.AddHostedService<TimeOffLeaveTypeWorkerService>();


        }

        private void DecryptAppSettings(IConfiguration configuration, string key, string[] regionsToDecrypt)
        {
            foreach (var region in regionsToDecrypt)
            {
                var regionValue = Configuration.GetValue<string>(region);
                if (!string.IsNullOrEmpty(regionValue))
                {
                    string decryptedRegion = DecryptData(regionValue, key);
                    configuration[region] = decryptedRegion;
                }
            }
        }

        private string DecryptData(string encryptedData, string key)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(key);
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                using (var decryptor = aes.CreateDecryptor())
                {
                    byte[] encryptedBytes = Convert.FromBase64String(encryptedData);
                    byte[] decryptedBytes = decryptor.TransformFinalBlock(encryptedBytes, 0, encryptedBytes.Length);
                    return Encoding.UTF8.GetString(decryptedBytes).TrimEnd('\0');
                }
            }
        }
    }
}

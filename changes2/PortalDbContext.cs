using ITTPortal.Core.BuyersPortal.Entities;
using ITTPortal.Core.Entities;
using ITTPortal.Core.Entities.Holidays;
using ITTPortal.Core.Entities.Sievo;
using ITTPortal.Core.Entities.SSA;
using ITTPortal.Infrastructure.Configurations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace ITTPortal.Infrastructure
{
    public class PortalDbContext : DbContext
    {
        private readonly IConfiguration _config;

        public PortalDbContext() : base() { }
        public PortalDbContext(DbContextOptions<PortalDbContext> options, IConfiguration config) : base(options)
        {
            _config = config;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(
                _config.GetConnectionString("DefaultConnection"),
                sqlServerOptionsAction: sqlOptions =>
                {
                    sqlOptions.EnableRetryOnFailure();
                    //maxRetryCount: 10,
                    //maxRetryDelay: TimeSpan.FromSeconds(5),
                    //errorNumbersToAdd: null);
                }
            );
        }

        public DbSet<Role> Roles { get; set; }
        public DbSet<ParLogs> ParLogs { get; set; }
        public DbSet<UserRole> UserRoles { get; set; }
        public DbSet<UserModule> UserModules { get; set; }
        public DbSet<Permission> Permissions { get; set; }
        public DbSet<RolePermission> RolePermissions { get; set; }
        public DbSet<Usr> Users { get; set; }
        public DbSet<Country> Country { get; set; }
        public DbSet<State> States { get; set; }
        public DbSet<City> Cities { get; set; }
        public DbSet<Employee> Employees { get; set; }
        public DbSet<EmployeeLeave> EmployeeLeaves { get; set; }
        public DbSet<LeaveRequest> LeaveRequests { get; set; }
        public DbSet<DayOff> DayOffs { get; set; }
        public DbSet<LeaveType> LeaveTypes { get; set; }
        public DbSet<LeaveTypeRule> LeaveTypeRules { get; set; }
        public DbSet<CountryEmployee> CountryEmployees { get; set; }
        public DbSet<BusinessUnit> BusinessUnits { get; set; }
        public DbSet<BusinessUnitEmployee> BusinessUnitEmployees { get; set; }
        public DbSet<ServiceType> ServiceTypes { get; set; }
        public DbSet<Office> Offices { get; set; }
        public DbSet<OfficeContact> OfficeContacts { get; set; }
        public DbSet<OfficeServiceArea> OfficeServiceAreas { get; set; }
        public DbSet<OfficeServiceAreaView> OfficeServiceAreaViews { get; set; }
        public DbSet<FaqCategory> FaqCategories { get; set; }
        public DbSet<Faq> Faqs { get; set; }
        public DbSet<ContactTicket> ContactTickets { get; set; }
        public DbSet<HitCounter> HitCounters { get; set; }
        public DbSet<County> Counties { get; set; }
        public DbSet<ZipCode> ZipCodes { get; set; }
        public DbSet<Announcement> Announcements { get; set; }
        public DbSet<RMARequest> RMARequests { get; set; }
        public DbSet<RMAStatus> RMAStatuses { get; set; }
        public DbSet<Disposition> Dispositions { get; set; }
        public DbSet<RMASalePerson> RMASalePersons { get; set; }
        public DbSet<SDARSupplier> SDARSuppliers { get; set; }
        public DbSet<CAPRequest> CAPRequests { get; set; }
        public DbSet<Udl> Udls { get; set; }
        public DbSet<UdlValue> UdlValues { get; set; }
        public DbSet<UdlDetailValue> UdlDetailValues { get; set; }
        public DbSet<Tag> Tags { get; set; }
        public DbSet<TagEmployee> TagEmployees { get; set; }
        public DbSet<TagViewEmployee> TagViewEmployees { get; set; }
        public DbSet<Supplier> Suppliers { get; set; }

        public DbSet<UdlValuePermission> UdlValuePermission { get; set; }
        public DbSet<RulesControls> RulesControls { get; set; }
        public DbSet<CronJob> CronJobs { get; set; }

        public DbSet<CronJobServiceBus> CronJobServiceBus { get; set; }
        public DbSet<UserProfile> UserProfiles { get; set; }

        public DbSet<Question> Questions { get; set; }
        public DbSet<Answer> Answer { get; set; }
        public DbSet<BaseSupplier> BaseSuppliers { get; set; }
        public DbSet<SSASupplier> SSASuppliers { get; set; }
        public DbSet<SupplierContact> SupplierContacts { get; set; }
        public DbSet<SSAApprover> SSAApprovers { get; set; }
        public DbSet<SSAHighRiskTeam> SSAHighRiskTeams { get; set; }
        public DbSet<SSASupplierApprover> SSASupplierApprovers { get; set; }

        public DbSet<SSASupplierValueCenter> SSASupplierValueCenters { get; set; }
        public DbSet<RenewalSurvey> SSARenewalSurveys { get; set; }

        public DbSet<ReSubmittedLegalSurvey> ReSubmittedLegalSurveys { get; set; }
        public DbSet<LegalSurvey> LegalSurveys { get; set; }
        public DbSet<RenewalLegalSurvey> RenewalLegalSurveys { get; set; }
        public DbSet<SSALegalSurveyUploadedDocument> SSALegalSurveyUploadedDocuments { get; set; }

        public DbSet<SSAUploadedDocument> SSAUploadedDocument { get; set; }
        public DbSet<BaseUploadedDocument> UploadedDocument { get; set; }
        public DbSet<ReSubmitedSurvey> ReSubmitedSurveys { get; set; }
        public DbSet<CLMSupplier> CLMSuppliers { get; set; }
        public DbSet<CLMOwner> CLMOwners { get; set; }
        public DbSet<CLMVcItLeader> CLMVcItLeaders { get; set; }
        public DbSet<CLMItLtLeader> CLMItLtLeaders { get; set; }
        public DbSet<CLMItamEmployee> CLMItamEmployees { get; set; }
        public DbSet<CLMGscTeam> CLMGSCTeams { get; set; }
        public DbSet<CLMStakeholder> CLMStakeholders { get; set; }
        public DbSet<CLMSupplierOwner> CLMSupplierOwners { get; set; }
        public DbSet<CLMSupplierStakeHolder> CLMSupplierStakeHolders { get; set; }
        public DbSet<CLMContract> CLMContracts { get; set; }
        public DbSet<CLMContractHistory> CLMContractHistories { get; set; }
        public DbSet<CLMContractEmailLog> CLMContractEmailLogs { get; set; }
        public DbSet<CLMContractReminder> CLMContractReminders { get; set; }
        public DbSet<CLMContractObligation> CLMContractObligations { get; set; }
        public DbSet<CLMContractDocument> CLMContractDocuments { get; set; }
        public DbSet<CLMContractStakeholder> CLMContractStakeholders { get; set; }
        public DbSet<CLMContractVC> CLMContractValueCenters { get; set; }
        public DbSet<CLMProductCategory> CLMProductCategories { get; set; }
        public DbSet<CLMDepartment> CLMDepartments { get; set; }
        public DbSet<CLMContractEvent> CLMContractEvents { get; set; }
        public DbSet<CLMContractEventNote> CLMContractEventNotes { get; set; }
        public DbSet<CLMContractEventTask> CLMContractEventTasks { get; set; }
        public DbSet<CLMContractEventTaskNote> CLMContractEventTaskNotes { get; set; }
        public DbSet<CLMContractEventHist> CLMContractEventHistories { get; set; }
        public DbSet<CLMRenewalType> CLMRenewTypes { get; set; }
        public DbSet<CLMJobProfileApproval> CLMJobProfileApprovals { get; set; }

        public DbSet<CodeOfConduct> CodeOfConduct { get; set; }
        public DbSet<SupplierRequirement> SupplierRequirements { get; set; }
        public DbSet<RequirementUploadedDocument> RequirementUploadedDocuments { get; set; }
        public DbSet<SupplierRequirementResponse> SupplierRequirementResponses { get; set; }
        public DbSet<RequirementResponseException> RequirementResponseExceptions { get; set; }
        public DbSet<SSASupplierSite> SSASupplierSites { get; set; }
        public DbSet<SSASupplierBusinessUnit> SSASupplierBusinessUnits { get; set; }
        public DbSet<SSARequirementValueCenter> SSARequirementValueCenters { get; set; }
        public DbSet<SSARequirementSites> SSARequirementSites { get; set; }
        public DbSet<SSARequirementBusinessUnit> SSARequirementBusinessUnits { get; set; }
        public DbSet<SSASite> SSASites { get; set; }
        public DbSet<SSAFaq> SSAFaqs { get; set; }
        public DbSet<SSAFaqCategory> SSAFaqCategories { get; set; }

        public DbSet<SSAFaqCategoryTypes> SSAFaqCategoryTypes { get; set; }
        public DbSet<ContactUsRequest> ContactUsRequests { get; set; }
        public DbSet<SSARegisterRequest> SSARegisterRequests { get; set; }
        public DbSet<SSASiteBusinessUnit> SSASiteBusinessUnits { get; set; }
        public DbSet<SSABusinessUnit> SSABusinessUnits { get; set; }
        public DbSet<SSABusinessUnitValueCenter> SSABusinessUnitValueCenters { get; set; }
        public DbSet<Calendar> Calendars { get; set; }
        public DbSet<Project> Projects { get; set; }
        public DbSet<ProjectTask> ProjectTasks { get; set; }
        public DbSet<ProjectTaskSyncFusion> ProjectTaskSyncFusion { get; set; }
        public DbSet<ProjectResource> ProjectResources { get; set; }
        public DbSet<TaskDependency> TaskDependencies { get; set; }
        public DbSet<TaskAssignee> TaskAssignees { get; set; }
        public DbSet<ProjectAssignee> ProjectAssignees { get; set; }
        public DbSet<ProjectDocument> ProjectDocuments { get; set; }
        public DbSet<ProjectDocCategory> ProjectDocCategories { get; set; }
        public DbSet<SSACompanyProfile> SSACompanyProfiles { get; set; }
        public DbSet<SSACompanyProfileUploadedDocument> SSACompanyProfileUploadedDocuments { get; set; }
        public DbSet<SSASupplierDocument> SSASupplierDocuments { get; set; }
        //public DbSet<SSASupplierDocumentResponse> SSASupplierDocumentResponses { get; set; }
        public DbSet<SSASupplierDocumentBusinessUnit> SSASupplierDocumentBusinessUnits { get; set; }
        public DbSet<SSASupplierDocumentSite> SSASupplierDocumentSites { get; set; }
        public DbSet<SSASupplierDocumentValueCenter> SSASupplierDocumentValueCenters { get; set; }
        public DbSet<SSASupplierDocumentUploadedDocument> SSASupplierDocumentUploadedDocuments { get; set; }

        public DbSet<RenewalSSADocument> RenewalSSADocuments { get; set; }
        public DbSet<SSASupplierNotification> SSASupplierNotifications { get; set; }
        public DbSet<SSASupplierNotificationSupplier> SSASupplierNotificationSuppliers { get; set; }
        public DbSet<SSASupplierNotificationUploadedDocument> SSASupplierNotificationUploadedDocuments { get; set; }
        public DbSet<UserAssignment> UserAssignments { get; set; }

        public DbSet<PMNSupplier> PMNSuppliers { get; set; }
        public DbSet<PMNApprover> PMNApprovers { get; set; }
        public DbSet<PMNSupplierApprover> PMNSupplierAppovers { get; set; }
        public DbSet<PMNSupplierDocument> PMNSupplierDocuments { get; set; }
        public DbSet<PMNSupplierDocEmailLog> PMNSupplierDocEmailLogs { get; set; }
        public DbSet<PMNDocumentType> PMNDocumentTypes { get; set; }
        public DbSet<PMNPart> PMNParts { get; set; }
        public DbSet<PMNPartApprover> PMNPartAppovers { get; set; }
        public DbSet<PMNPartDocument> PMNPartDocuments { get; set; }

        public DbSet<PMNSupplierPart> PMNSupplierParts { get; set; }
        public DbSet<PMNSupplierPartChangeAction> PMNSupplierPartChangeActions { get; set; }
        public DbSet<PMNSupplierPartType> PMNSupplierPartTypes { get; set; }
        public DbSet<PMNSupplierPartTypeDoc> PMNSupplierPartTypeDocs { get; set; }
        public DbSet<PMNSupplierPartDelivery> PMNSupplierPartDeliveries { get; set; }
        public DbSet<PMNSupplierPartDeliveryDoc> PMNSupplierPartDeliveryDocs { get; set; }


        public DbSet<Calibration> Calibrations { get; set; }
        public DbSet<CalibrationEmployee> CalibrationEmployees { get; set; }
        public DbSet<CalibrationYear> CalibrationYears { get; set; }
        public DbSet<FRCUploadedDocument> FRCUploadedDocuments { get; set; }
        public DbSet<FRCFaq> FRCFaqs { get; set; }
        public DbSet<FRCFaqCategory> FRCFaqCategories { get; set; }
        public DbSet<FRCFaqCategoryTypes> FRCFaqCategoryTypes { get; set; }

        public DbSet<SupplierShareFile> SupplierShareFiles { get; set; }
        public DbSet<SupplierShareFileAccessRule> SupplierShareFileAccessRules { get; set; }

        public DbSet<FRCPaymentHistory> FRCPaymentHistory { get; set; }
        public DbSet<FRCInvoiceStatus> FRCInvoiceStatus { get; set; }
        public DbSet<FRCVendorInfo> FRCVendorInfo { get; set; }
        public DbSet<FRCCurYrInvoices> FRCCurYrInvoices { get; set; }
        public DbSet<FRCInvoicePoStatus> FRCInvoicePoStatus { get; set; }
        public DbSet<FRCInvoiceLineItem> FRCInvoiceLineItem { get; set; }
        public DbSet<FRCDolApT> FRCDolApT { get; set; }
        public DbSet<FRCValueCentersFilter> FRCValueCentersFilter { get; set; }
        public DbSet<FRCBusinessUnitsFilter> FRCBusinessUnitsFilter { get; set; }
        public DbSet<FRCVendorNamesFilter> FRCVendorNamesFilter { get; set; }
        public DbSet<FRCVendorNumFilter> FRCVendorNumFilter { get; set; }
        public DbSet<FRCDocumentStatusFilter> FRCDocumentStatusFilter { get; set; }
        public DbSet<FRCDocumentNumberFilter> FRCDocumentNumberFilter { get; set; }

        public DbSet<FRCCompaniesFilter> FRCCompaniesFilter { get; set; }

        public DbSet<FRCCreditStatusFilter> FRCCreditStatusFilter { get; set; }
        public DbSet<FRCCustNoFilter> FRCCustNoFilter { get; set; }
        public DbSet<FRCOsEntityFilter> FRCOsEntityFilter { get; set; }
        public DbSet<FRCPurOrderFilter> FRCPurOrderFilter { get; set; }
        public DbSet<FRCRefDocNoFilter> FRCRefDocNoFilter { get; set; }
        public DbSet<FRCSalesAreaFilter> FRCSalesAreaFilter { get; set; }
        public DbSet<FRCSalesAreaDescFilter> FRCSalesAreaDescFilter { get; set; }
        public DbSet<FRCStatusDescFilter> FRCStatusDescFilter { get; set; }
        public DbSet<FRCCustomerInfo> FRCCustomerInfo { get; set; }
        public DbSet<FRCCustomerFinancialSummary> FRCCustomerFinancialSummary { get; set; }
        public DbSet<FRCCustomerFinancialDetail> FRCCustomerFinancialDetail { get; set; }
        public DbSet<FRCArRowLvlSecurity> FRCArRowLvlSecurity { get; set; }
        public DbSet<FRCApRowLvlSecurity> FRCApRowLvlSecurity { get; set; }
        public DbSet<FRCTeamMember> FRCTeamMember { get; set; }
        public DbSet<FRCCalendarDate> FRCCalendarDates { get; set; }
        public DbSet<EmployeeSignature> EmployeeSignatures { get; set; }
        public DbSet<EAModule> EAModules { get; set; }
        public DbSet<EAProcess> EAProcesses { get; set; }
        public DbSet<EAProcessActor> EAProcessActors { get; set; }
        public DbSet<EAProcessRequest> EAProcessRequests { get; set; }
        public DbSet<EAProcessRequestTransLog> EAProcessRequestTransLogs { get; set; }
        public DbSet<EAProcessStatus> EAProcessStatuses { get; set; }
        public DbSet<EATemplate> EATemplates { get; set; }
        public DbSet<EATemplateField> EATemplateFields { get; set; }

        #region Ongoarding
        public DbSet<OnboardingEmployee> OnboardingEmployees { get; set; }
        public DbSet<OnboardingResource> OnboardingResources { get; set; }
        public DbSet<JobFamily> JobFamilies { get; set; }

        public DbSet<JobFamilySite> JobFamilySites { get; set; }


        public DbSet<JobFamilyGroup> JobFamilyGroups { get; set; }
        public DbSet<JobProfile> JobProfiles { get; set; }
        public DbSet<OnboardingSite> OnboardingSites { get; set; }
        public DbSet<OnboardingJobFamilyResource> JobFamilyResources { get; set; }

        public DbSet<OnboardingEmployeeJobFamilyResource> OnboardingEmployeeJobFamilyResources { get; set; }

        public DbSet<OnboardingResourceDetail> OnboardingResourceDetails { get; set; }
        public DbSet<OnboardingResourceValueCenter> OnboardingResourceValueCenters { get; set; }
        public DbSet<OnboardingResourceDetailSite> OnboardingResourceDetailSites { get; set; }
        public DbSet<OnboardingEmployeeJobFamilyResource> OnboardingEmployeeResources { get; set; }
        public DbSet<OnboardingEmployeeResourcesSnapShot> OnboardingEmployeeResourcesSnapShots { get; set; }
        public DbSet<EmployeeValueCenter> EmployeeValueCenters { get; set; }
        public DbSet<OnboardingResourceSite> OnboardingResourceSites { get; set; }
        #endregion

        #region PCN

        public DbSet<PCNCategory> PCNCategories { get; set; }
        public DbSet<PCNDepartment> PCNDepartments { get; set; }
        public DbSet<PCNEmployee> PCNEmployees { get; set; }
        public DbSet<PCNEmailEmployee> PCNEmailEmployees { get; set; }

        public DbSet<PCNProject> PCNProjects { get; set; }
        public DbSet<PCNProjectDocument> PCNProjectDocuments { get; set; }
        public DbSet<PCNProjectCategory> PCNProjectCategories { get; set; }
        public DbSet<PCNProjectPhase> PCNProjectPhases { get; set; }
        public DbSet<PCNProjectPhaseDoc> PCNProjectPhaseDocs { get; set; }

        #endregion

        #region CCA
        public DbSet<CCAEmployee> CCAEmployees { get; set; }
        public DbSet<CCAEmailEmployee> CCAEmailEmployees { get; set; }
        public DbSet<CCACostCenter> CCACostCenters { get; set; }
        public DbSet<CCACostCenterHistory> CCACostCenterHistories { get; set; }
        public DbSet<CCAActivityType> CCAActivityTypes { get; set; }
        public DbSet<CCAAccount> CCAAccounts { get; set; }
        public DbSet<CCATransaction> CCATransactions { get; set; }
        public DbSet<CCATransactionDocument> CCATransactionDocuments { get; set; }
        public DbSet<CCATransactionContract> CCATransactionContracts { get; set; }
        public DbSet<CCACostDriver> CCACostDrivers { get; set; }
        public DbSet<CCACostAllocation> CCACostAllocations { get; set; }
        public DbSet<CCASiteCode> CCASiteCodes { get; set; }
        public DbSet<CCAAccountSC> CCAAccountSCs { get; set; }

        #endregion

        #region PPM
        //public DbSet<PPMStep> PPMSteps { get; set; }
        //public DbSet<PPMTeam> PPMTeams { get; set; }
        public DbSet<PPMTeamMember> PPMTeamMembers { get; set; }
        public DbSet<PPMDepartment> PPMDepartments { get; set; }
        public DbSet<PPMSite> PPMSites { get; set; }
        public DbSet<PPMBusinessUnit> PPMBusinessUnits { get; set; }
        public DbSet<PPMProject> PPMProjects { get; set; }
        public DbSet<PPMProjectTransactionLog> PPMProjectTransactionLogs { get; set; }
        public DbSet<PPMProjectUploadedDocument> PPMProjectUploadedDocuments { get; set; }
        public DbSet<PPMProjectBPOStakeholderLog> PPMProjectBPOStakeholderLogs { get; set; }
        public DbSet<PPMProjectITReviewerLog> PPMProjectITReviewerLogs { get; set; }
        public DbSet<PPMCriteriaWeight> PPMCriteriaWeights { get; set; }
        public DbSet<PPMProjectScore> PPMProjectScores { get; set; }
        public DbSet<PPMValueCenter> PPMValueCenters { get; set; }
        public DbSet<PPMProjectType> PPMProjectTypes { get; set; }
        public DbSet<PPMBRMEmployee> PPMBRMEmployees { get; set; }
        public DbSet<PPMBusinessUnitLocation> PPMBusinessUnitLocations { get; set; }

        #endregion

        #region Sievo Sets

        public DbSet<SievoSupplier> SP_DEV_VM { get; set; }
        public DbSet<SievoSupplierLocation> SP_DEV_WG { get; set; }
        //public DbSet<SievoSupplierPartInfo> SievoSupplierPartsInfo { get; set; }
        #endregion

        #region Entity List
        public DbSet<EntityListDetail> EntityLists { get; set; }
        #endregion


        #region Document Storage Sets 

        DbSet<FileMeta> FilesMeta { get; set; }
        DbSet<FileBytes> FilesBytes { get; set; }
        #endregion


        #region POApprovals

        public DbSet<Core.Entities.POApprovals.PoStgHeader> PoStgHeaders { get; set; }
        public DbSet<Core.Entities.POApprovals.PoStgLine> PoStgLines { get; set; }
        public DbSet<Core.Entities.POApprovals.SqlDelegationOfAuthorityDirectMaterial> PoDelegationOfAuthorityDirectMaterials { get; set; }
        public DbSet<Core.Entities.POApprovals.SqlDelegationOfAuthorityIndirectExpense> PoDelegationOfAuthorityIndirectExpenses { get; set; }
        public DbSet<Core.Entities.POApprovals.PoApprovalChain> PoApprovalChains { get; set; }
        public DbSet<Core.Entities.POApprovals.PoApprovalStage> PoApprovalStages { get; set; }
        public DbSet<Core.Entities.POApprovals.PoApprovalOutbox> PoApprovalOutboxes { get; set; }
        public DbSet<Core.Entities.POApprovals.PoApprovalAudit> PoApprovalAudits { get; set; }
        public DbSet<Core.Entities.POApprovals.PoApproverDirectory> PoApproverDirectories { get; set; }
        public DbSet<Core.Entities.POApprovals.PoHeader> PoHeaders { get; set; }
        // Read-only views
        public DbSet<Core.Entities.POApprovals.Views.PoHeaderView> PoHeaderViews { get; set; }
        public DbSet<Core.Entities.POApprovals.Views.PoLineView> PoLineViews { get; set; }


        #endregion

        public DbSet<SSASelfEvaluation> SSASelfEvaluations { get; set; }
        public DbSet<SSASelfEvaluationUploadedDocument> SSASelfEvaluationUploadedDocuments { get; set; }
        public DbSet<SupplierPartInfo> SuppliersPartInfo { get; set; }
        public DbSet<SSANDA> SSANDA { get; set; }
        public DbSet<SSANDAValueCenter> SSANDAValueCenter { get; set; }
        public DbSet<SSANDADocument> SSANDADocument { get; set; }
        public DbSet<SSANDADocumentBusinessUnit> SSANDADocumentBusinessUnit { get; set; }
        public DbSet<SSANDADocumentValueCenter> SSANDADocumentValueCenter { get; set; }
        public DbSet<SSANDADocumentSite> SSANDADocumentSite { get; set; }
        public DbSet<SSANDABusinessUnit> SSANDABusinessUnit { get; set; }
        public DbSet<SSANDASite> SSANDASite { get; set; }
        public DbSet<SSANDARequest> SSANDARequest { get; set; }
        public DbSet<SSANDARequestDocument> SSANDARequestDocument { get; set; }

        public DbSet<EntityListDetail> EntityListDetails { get; set; }
        public DbSet<EntityListHcue> EntityListHcues { get; set; }
        public DbSet<EntityListHcueAddress> EntityListHcueAddresses { get; set; }
        public DbSet<EntityListHcueFormerName> EntityListHcueFormerNames { get; set; }
        public DbSet<EntityListHcueOwner> EntityListHcueOwners { get; set; }
        public DbSet<EntityListHcueRelatedName> EntityListHcueRelatedNames { get; set; }
        public DbSet<EntityListHcueOrganizationHistory> EntityListHcueOrganizationHistories { get; set; }
        public DbSet<EntityListOneStream> EntityListOneStreams { get; set; }
        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.ApplyConfiguration(new RoleConfiguration());
            builder.ApplyConfiguration(new RolePermissionConfiguration());
            builder.ApplyConfiguration(new CountryEmployeeConfiguration());
            builder.ApplyConfiguration(new TagEmployeeConfiguration());
            builder.ApplyConfiguration(new TagViewEmployeeConfiguration());
            builder.ApplyConfiguration(new CLMContractVCConfiguration());
            builder.ApplyConfiguration(new BusinessUnitEmployeeConfiguration());
            builder.ApplyConfiguration(new UserModuleConfiguration());
            builder.ApplyConfiguration(new PCNProjectCategoryConfiguration());
            builder.ApplyConfiguration(new JobFamilyResourceConfiguration());
            builder.ApplyConfiguration(new CCAAccountSCConfiguration());

            // POApprovals configurations
            builder.ApplyConfiguration(new POApprovalsPoStgHeaderConfiguration());
            builder.ApplyConfiguration(new POApprovalsPoStgLineConfiguration());
            builder.ApplyConfiguration(new POApprovalsDelegationOfAuthorityDirectMaterialConfiguration());
            builder.ApplyConfiguration(new POApprovalsDelegationOfAuthorityIndirectExpenseConfiguration());
            builder.ApplyConfiguration(new POApprovalsApprovalChainConfiguration());
            builder.ApplyConfiguration(new POApprovalsApprovalStageConfiguration());
            builder.ApplyConfiguration(new POApprovalsApprovalOutboxConfiguration());
            builder.ApplyConfiguration(new POApprovalsApprovalAuditConfiguration());
            builder.ApplyConfiguration(new PoHeaderViewConfiguration());
            builder.ApplyConfiguration(new PoLineViewConfiguration());
            builder.ApplyConfiguration(new PoApprovalsPoApproverDirectoryConfiguration());
            builder.ApplyConfiguration(new PoApprovalsPoHeaderConfiguration());


            builder.Entity<Usr>()
                 .HasMany(p => p.ShareFileAccessRules)
                 .WithOne()
                 .HasForeignKey(p => p.UserId);

            builder.Entity<SupplierShareFile>()
                 .HasMany(p => p.AccessRules)
                 .WithOne()
                 .HasForeignKey(p => p.FileId);

            builder.Entity<OfficeServiceAreaView>().ToView("OfficeServiceAreaView");
            builder.Entity<OfficeServiceAreaView>().HasNoKey(); // Views are typically read-only

            // builder.ApplyConfiguration(new PermissionConfiguration());
            // builder.ApplyConfiguration(new UserRoleConfiguration());
            // builder.ApplyConfiguration(new UserConfiguration());
        }
    }
}
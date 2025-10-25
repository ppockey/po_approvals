USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_Approval_Audit]    Script Date: 10/25/2025 9:02:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_Approval_Audit](
	[AuditId] [bigint] IDENTITY(1,1) NOT NULL,
	[PoNumber] [nvarchar](20) NOT NULL,
	[OldStatus] [char](1) NOT NULL,
	[NewStatus] [char](1) NOT NULL,
	[ChangedBy] [nvarchar](100) NOT NULL,
	[ChangedAtUtc] [datetime2](0) NOT NULL,
	[DecisionNote] [nvarchar](4000) NULL,
PRIMARY KEY CLUSTERED 
(
	[AuditId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_Approval_Audit] ADD  CONSTRAINT [DF_PO_Approval_Audit_ChangedAtUtc]  DEFAULT (sysutcdatetime()) FOR [ChangedAtUtc]
GO



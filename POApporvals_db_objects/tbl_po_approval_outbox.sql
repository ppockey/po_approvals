USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_ApprovalOutbox]    Script Date: 10/26/2025 9:15:20 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_ApprovalOutbox](
	[OutboxId] [bigint] IDENTITY(1,1) NOT NULL,
	[EventType] [nvarchar](40) NOT NULL,
	[PoNumber] [nvarchar](20) NOT NULL,
	[OccurredAtUtc] [datetime2](0) NOT NULL,
	[PayloadJson] [nvarchar](max) NULL,
	[Attempts] [int] NOT NULL,
	[ProcessedAtUtc] [datetime2](0) NULL,
	[DirectAmount] [decimal](18, 2) NULL,
	[IndirectAmount] [decimal](18, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[OutboxId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_ApprovalOutbox] ADD  CONSTRAINT [DF_PO_ApprovalOutbox_Occurred]  DEFAULT (sysutcdatetime()) FOR [OccurredAtUtc]
GO

ALTER TABLE [dbo].[PO_ApprovalOutbox] ADD  CONSTRAINT [DF_PO_ApprovalOutbox_Attempts]  DEFAULT ((0)) FOR [Attempts]
GO



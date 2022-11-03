SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[StageDiscovery]
(
   @IngestionBatchId AS NVARCHAR(200)
)
AS
BEGIN
	--Declare @IngestionBatchId  AS NVARCHAR(200) = '12345'

	UPDATE SqlIngestion
		SET 
			[SqlIngestion].[IsHistoryTable] = [SqlIngestion_DiscoveryStaging].[IsHistoryTable],
			[SqlIngestion].[IsChangeTrackingEnabled] = [SqlIngestion_DiscoveryStaging].[IsChangeTrackingEnabled],
			[SqlIngestion].[RowCount] = [SqlIngestion_DiscoveryStaging].[RowCount],
			[SqlIngestion].[ConnectionString] = [SqlIngestion_DiscoveryStaging].[ConnectionString],
			[SqlIngestion].[SystemName] = [SqlIngestion_DiscoveryStaging].[SystemName]
		FROM [dbo].[SqlIngestion_DiscoveryStaging]
			Inner Join SqlIngestion on SqlIngestion.[Server] = [SqlIngestion_DiscoveryStaging].[Server]
				AND  SqlIngestion.[Database] = [SqlIngestion_DiscoveryStaging].[Database]
				AND  SqlIngestion.[Schema] = [SqlIngestion_DiscoveryStaging].[Schema]
				AND  SqlIngestion.[Table] = [SqlIngestion_DiscoveryStaging].[Table]
		Where IngestionBatchId = @IngestionBatchId

	
	INSERT INTO SqlIngestion  
			([Server],
			[Database],
			[Schema],
			[Table],
			[IsHistoryTable],
			[IsChangeTrackingEnabled],
			[RowCount],
			[IsIngestionEnabled])
		SELECT	[SqlIngestion_DiscoveryStaging].[Server],
				[SqlIngestion_DiscoveryStaging].[Database],
				[SqlIngestion_DiscoveryStaging].[Schema],
				[SqlIngestion_DiscoveryStaging].[Table],
				[SqlIngestion_DiscoveryStaging].[IsHistoryTable],
				[SqlIngestion_DiscoveryStaging].[IsChangeTrackingEnabled],
				[SqlIngestion_DiscoveryStaging].[RowCount],
				0
			FROM [dbo].[SqlIngestion_DiscoveryStaging]
				Left Join SqlIngestion on SqlIngestion.[Server] = [SqlIngestion_DiscoveryStaging].[Server]
					AND  SqlIngestion.[Database] = [SqlIngestion_DiscoveryStaging].[Database]
					AND  SqlIngestion.[Schema] = [SqlIngestion_DiscoveryStaging].[Schema]
					AND  SqlIngestion.[Table] = [SqlIngestion_DiscoveryStaging].[Table]
			WHERE IngestionBatchId = @IngestionBatchId AND SqlIngestion.Pk is null


	UPDATE SqlIngestion
		SET 
			[SqlIngestion].[IsIngestionEnabled] = 1
		FROM [dbo].[SqlIngestion]
		Where ([IsIngestionEnabled] = 1 OR [IsIngestionEnabled] IS NULL) AND [SqlIngestion].[RowCount] > 0 AND [SqlIngestion].IsHistoryTable = 0

	DELETE FROM [dbo].[SqlIngestion_DiscoveryStaging]
		Where IngestionBatchId = @IngestionBatchId

END

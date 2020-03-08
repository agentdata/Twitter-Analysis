-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop tables, procedures and schema   ?for development?
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--delete from TwitterAssignmentDB.TweetData
--GO
--DROP PROCEDURE IF EXISTS [TwitterAssignmentDB].[AddSentimentName];
--GO
--DROP PROCEDURE IF EXISTS [TwitterAssignmentDB].[insertTweet];
--GO
--DROP PROCEDURE IF EXISTS [TwitterAssignmentDB].[AddSentimentScoreAndKeyPhrases];
--GO
--DROP TABLE IF EXISTS TwitterAssignmentDB.TweetData;
--GO
--DROP TABLE IF EXISTS TwitterAssignmentDB.SentimentValue;
--GO
--DROP Schema IF EXISTS TwitterAssignmentDB;
--GO


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- create schema and tables
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

CREATE Schema TwitterAssignmentDB
GO

CREATE 
	Table TwitterAssignmentDB.TweetData
	(
		DBTweetID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
		TwitterTweetID varchar(max) NOT NULL,
		SearchTerm varchar(max)NOT NULL,
		TweetFullText varchar(max) NOT NULL,
		KeyPhrases varchar(max) NULL,
		TwitterUserName varchar(max),
		TweetLocation varchar(max),
		TweetTime DateTime,
		NumberOfRetweets int,
		FavoriteCount int,
		SourceApp varchar(max),
		SentimentScore FLOAT NULL,
		SentimentID int NULL
	);
GO

CREATE
    Table TwitterAssignmentDB.SentimentValue
	(
		SentimentID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
		SentimentName varchar(max)
	);
GO

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Stored procedures
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE [TwitterAssignmentDB].[insertTweet]
@TwitterTweetID varchar(max),
@SearchTerm varchar(max),
@TweetFullText varchar(max),
@TwitterUserName varchar(max),
@TweetLocation varchar(max),
@TweetTime DateTime,
@NumberOfRetweets int,
@FavoriteCount int,
@SourceApp varchar(max)
AS
BEGIN
	--If the same twittertweetid doesn't exist then insert, otherwise do nothing
	IF ((SELECT count(*) FROM [TwitterAssignmentDB].[TweetData] WHERE TwitterTweetID = @TwitterTweetID) = 0)
		BEGIN
			INSERT INTO [TwitterAssignmentDB].[TweetData] (TwitterTweetID, SearchTerm, TweetFullText, TwitterUserName, TweetLocation, TweetTime, NumberOfRetweets, FavoriteCount, SourceApp)
				VALUES(@TwitterTweetID, @SearchTerm, @TweetFullText, @TwitterUserName, @TweetLocation, @TweetTime, @NumberOfRetweets, @FavoriteCount, @SourceApp)
		END
END;
GO

CREATE OR ALTER PROCEDURE [TwitterAssignmentDB].[AddSentimentName]
@TweetID INT,
@SentimentScore FLOAT
AS
BEGIN TRY
	DECLARE @sentimentID int = -1
	IF @SentimentScore = .5
		BEGIN
			SET @SentimentID = 3
		END
	ELSE
		BEGIN
			IF @SentimentScore > .5
				BEGIN
					IF @SentimentScore > .75
						SET @sentimentID = 5
					IF @SentimentScore <= .75
						SET @sentimentID = 4
				END
			IF @SentimentScore < .5
				BEGIN
					IF @SentimentScore < .25
						SET @sentimentID =  1
					IF @SentimentScore >= .25
						SET @sentimentID =  2
				END
		END
	IF @sentimentID > 0
		BEGIN
			UPDATE TwitterAssignmentDB.TweetData SET SentimentID = @sentimentID WHERE DBTweetID = @TweetID
		END
END TRY
BEGIN CATCH

END CATCH;
GO

CREATE OR ALTER PROCEDURE [TwitterAssignmentDB].[AddSentimentScoreAndKeyPhrases]
@TweetID varchar(max),
@SentimentScore FLOAT,
@Key_Phrases varchar(max)
AS
BEGIN TRY
	if (SELECT COUNT(*) FROM TwitterAssignmentDB.TweetData WHERE TwitterTweetID = @TweetID) > 0
		BEGIN
			UPDATE TwitterAssignmentDB.TweetData SET SentimentScore = @SentimentScore, KeyPhrases = @Key_Phrases WHERE TwitterTweetID = @TweetID
		END
END TRY
BEGIN CATCH

END CATCH;
GO

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Triggers
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

CREATE OR ALTER TRIGGER [TwitterAssignmentDB].[AddSentimentScore]
    ON [TwitterAssignmentDB].[TweetData]
AFTER UPDATE
AS
	BEGIN
		IF UPDATE(SentimentScore)
			BEGIN
				DECLARE @TweetID varchar(max)= (SELECT DBTweetID FROM INSERTED) 
				DECLARE @SentimentScore varchar(max) = (SELECT SentimentScore FROM INSERTED)
				EXEC TwitterAssignmentDB.AddSentimentName @TweetID = @tweetid , @SentimentScore = @sentimentscore
			END
	END;
GO

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- insert statements neutral, very positive, very negative, slightly positive, slightly negative
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
SET IDENTITY_INSERT TwitterAssignmentDB.SentimentValue ON;
GO
INSERT INTO TwitterAssignmentDB.SentimentValue (SentimentID, SentimentName) VALUES(1, 'very negative');
INSERT INTO TwitterAssignmentDB.SentimentValue (SentimentID, SentimentName) VALUES(2, 'slightly negative');
INSERT INTO TwitterAssignmentDB.SentimentValue (SentimentID, SentimentName) VALUES(3, 'neutral');
INSERT INTO TwitterAssignmentDB.SentimentValue (SentimentID, SentimentName) VALUES(4, 'slightly positive');
INSERT INTO TwitterAssignmentDB.SentimentValue (SentimentID, SentimentName) VALUES(5, 'very positive');
GO
SET IDENTITY_INSERT TwitterAssignmentDB.SentimentValue OFF;
GO

ALTER TABLE TwitterAssignmentDB.TweetData
   ADD CONSTRAINT foreignkey FOREIGN KEY (SentimentID)
      REFERENCES TwitterAssignmentDB.SentimentValue (SentimentID);
GO


--SELECT
--TD.*, SV.SentimentName
--FROM 
--	(select * from TwitterAssignmentDB.TweetData)
--	AS TD
--	JOIN TwitterAssignmentDB.SentimentValue as SV
--		on (SV.SentimentID = TD.SentimentID)
--ORDER BY
--	TwitterTweetID


--select * from TwitterAssignmentDB.TweetData ORDER BY TwitterTweetID

--delete from TwitterAssignmentDB.TweetData

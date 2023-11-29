CREATE DATABASE `project`;

CREATE TABLE `project`.`channel` (
  `Channel_id` varchar(255) NOT NULL,
  `playlist_id` varchar(255) DEFAULT NULL,
  `Channel_Name` varchar(255) DEFAULT NULL,
  `Channel_subscribers` bigint DEFAULT NULL,
  `Channel_views` bigint DEFAULT NULL,
  `Channel_Description` longtext,
  `Channel_videos_Count` int DEFAULT NULL,
  `channel_details_date` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`Channel_id`)
);
CREATE TABLE `project`.`playlist` (
  `playlist_id` varchar(255) NOT NULL,
  `channel_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`playlist_id`),
  KEY `channel_id` (`channel_id`),
  CONSTRAINT `playlist_ibfk_1` FOREIGN KEY (`channel_id`) REFERENCES `channel` (`Channel_id`)
);
CREATE TABLE `project`.`video` (
  `Video_id` varchar(255) NOT NULL,
  `playlist_id` varchar(255) DEFAULT NULL,
  `Video_Name` varchar(255) DEFAULT NULL,
  `Video_Description` longtext,
  `Video_published_date` datetime DEFAULT NULL,
  `Video_views_count` bigint DEFAULT NULL,
  `Video_like_count` bigint DEFAULT NULL,
  `Video_favorite_count` bigint DEFAULT NULL,
  `Video_comment_count` bigint DEFAULT NULL,
  `Video_duartion` bigint DEFAULT NULL,
  `Video_thumbnail` varchar(255) DEFAULT NULL,
  `Video_caption_status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Video_id`),
  KEY `playlist_id` (`playlist_id`),
  CONSTRAINT `video_ibfk_1` FOREIGN KEY (`playlist_id`) REFERENCES `playlist` (`playlist_id`)
);
CREATE TABLE `project`.`comment` (
  `Comment_id` varchar(255) NOT NULL,
  `Video_id` varchar(255) DEFAULT NULL,
  `Comment_text` longtext,
  `Comment_author` varchar(255) DEFAULT NULL,
  `Comment_published_date` datetime DEFAULT NULL,
  `Comment_parent_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Comment_id`),
  KEY `Video_id` (`Video_id`),
  CONSTRAINT `comment_ibfk_1` FOREIGN KEY (`Video_id`) REFERENCES `video` (`Video_id`)
);

USE `project`;
CREATE  OR REPLACE VIEW `Video_names` AS
select vi.Video_Name,ch.Channel_Name from project.video vi 
inner join project.playlist pl on vi.playlist_id=pl.playlist_id 
inner join project.channel ch on pl.Channel_id=ch.channel_id;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`Maximum_videos` AS
   SELECT 
		`ch`.`Channel_Name` AS `Channel_Name`,
        count(`vi`.`Video_id`) as `Total_number_of_video`
        
    FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        group by ch.Channel_Name
        order by `Total_number_of_video` desc limit 1;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`vw_top_viewed_videos` AS
SELECT 
	`ch`.`Channel_Name` AS `Channel_Name`,
       `vi`.`Video_name` as `Video_name`,
      `vi`.`Video_views_count` as `Video_views_count`
        
    FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        order by `Video_views_count` desc limit 10;

USE `project`;

CREATE 
 OR REPLACE VIEW `project`.`vw_top_like_video` AS
SELECT 
	`ch`.`Channel_Name` AS `Channel_Name`,
	`vi`.`Video_name` as `Video_name`,
	`vi`.`Video_like_count` as `Video_like_count`
FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        order by `Video_like_count` desc limit 1;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`vw_channels_active_in_2022` AS
SELECT 
	distinct `ch`.`Channel_Name` AS `Channel_Name`
FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        where Year(vi.Video_published_date)=2022;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`vw_channels_avg_video_duration` AS
SELECT 
	`ch`.`Channel_Name` AS `Channel_Name`,
    avg(vi.Video_duartion) as avg_video_duration_in_mins 
FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        group by ch.channel_name
        order by avg_video_duration_in_mins desc;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`vw_channels_top_Video_comment_count` AS
SELECT 
	`ch`.`Channel_Name` AS `Channel_Name`,
    vi.Video_Name as Video_Name,
    vi.Video_comment_count as Video_comment_count
FROM
        ((`project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON ((`vi`.`playlist_id` = `pl`.`playlist_id`)))
        JOIN `project`.`channel` `ch` ON ((`pl`.`channel_id` = `ch`.`Channel_id`)))
        order by Video_comment_count desc limit 10;

USE `project`;
CREATE 
 OR REPLACE VIEW `project`.`vw_channels_top_Video_view_count_foreach` AS
select ch.channel_name,Video_Name,cc.max_Video_views_count from `project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON `vi`.`playlist_id` = `pl`.`playlist_id`
        JOIN `project`.`channel` `ch` ON `pl`.`channel_id` = `ch`.`Channel_id` join(
select channel_name,max(Video_views_count) as max_Video_views_count FROM
        `project`.`video` `vi`
        JOIN `project`.`playlist` `pl` ON `vi`.`playlist_id` = `pl`.`playlist_id`
        JOIN `project`.`channel` `ch` ON `pl`.`channel_id` = `ch`.`Channel_id` 
        group by channel_name
        ) cc on ch.channel_name=cc.Channel_Name and cc.max_Video_views_count=vi.Video_views_count 
        order by cc.max_Video_views_count desc;

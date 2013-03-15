CREATE TABLE `alarm` (
  `id` varchar(36) NOT NULL,
  `tenant_id` varchar(36) NOT NULL,
  `name` varchar(250) NOT NULL,
  `namespace` varchar(100) NOT NULL,
  `metric_type` varchar(50) NOT NULL,
  `metric_subject` varchar(50),
  `operator` varchar(5) NOT NULL check operator in ('LT','LTE','GT','GTE'),
  `threshold` bigint(11) NOT NULL,
  `state` varchar(20) NOT NULL check state in ('UNDETERMINED','OK','ALARM'),
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `alarm_dimension` (
  `alarm_id` varchar(36) NOT NULL,
  `dimension_name` varchar(50) NOT NULL,
  `value` varchar(300) NOT NULL,
  PRIMARY KEY (`alarm_id`,`dimension_name`),
);

CREATE TABLE `alarm_action` (
  `alarm_id` varchar(36) NOT NULL,
  `action_id` varchar(50) NOT NULL,
  PRIMARY KEY (`alarm_id`,`action_id`),
);
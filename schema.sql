CREATE TABLE IF NOT EXISTS `dailies`  -- READ-ONLY table, do not INSERT/UPDATE/DELETE besides automation
(
    `id`        INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
    `clock`     DATE DEFAULT (CURRENT_DATE),
    `BASE_delta` INT(11) NOT NULL DEFAULT 0,
    `HCHC_delta` INT(11) NOT NULL DEFAULT 0,
    `HCHP_delta` INT(11) NOT NULL DEFAULT 0,
    `EJPHN_delta` INT(11) NOT NULL DEFAULT 0,
    `EJPHPM_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJB_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJB_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJW_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJW_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJR_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJR_delta` INT(11) NOT NULL DEFAULT 0,

    PRIMARY KEY (id),
    KEY `clock` (`clock`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `stream` (
    `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
    `clock` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `ADCO` VARCHAR(12) NOT NULL DEFAULT '',
    `OPTARIF` VARCHAR(4) DEFAULT NULL,
    `ISOUSC` TINYINT(1) NOT NULL DEFAULT 0,
    `BASE` INT(11) NOT NULL DEFAULT 0,
    `BASE_delta` INT(11) NOT NULL DEFAULT 0,
    `HCHC` INT(11) NOT NULL DEFAULT 0,
    `HCHC_delta` INT(11) NOT NULL DEFAULT 0,
    `HCHP` INT(11) NOT NULL DEFAULT 0,
    `HCHP_delta` INT(11) NOT NULL DEFAULT 0,
    `EJPHN` INT(11) NOT NULL DEFAULT 0,
    `EJPHN_delta` INT(11) NOT NULL DEFAULT 0,
    `EJPHPM` INT(11) NOT NULL DEFAULT 0,
    `EJPHPM_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJB` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJB_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJB` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJB_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJW` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJW_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJW` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJW_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJR` INT(11) NOT NULL DEFAULT 0,
    `BBRHCJR_delta` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJR` INT(11) NOT NULL DEFAULT 0,
    `BBRHPJR_delta` INT(11) NOT NULL DEFAULT 0,
    `PEJP` SMALLINT(2) DEFAULT NULL,
    `PTEC` VARCHAR(4) DEFAULT NULL,
    `DEMAIN` VARCHAR(4) DEFAULT NULL,
    `IINST` SMALLINT(2) NOT NULL DEFAULT 0,
    `ADPS` SMALLINT(2) NOT NULL DEFAULT 0,
    `IMAX` SMALLINT(2) NOT NULL DEFAULT 0,
    `PAPP` MEDIUMINT(3) NOT NULL DEFAULT 0,
    `HHPHC` VARCHAR(1) DEFAULT NULL,
    `MOTDETAT` VARCHAR(6) DEFAULT NULL,

    PRIMARY KEY (id),
    KEY `clock` (`clock`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE VIEW IF NOT EXISTS contract AS
    SELECT ADCO,
           OPTARIF,
           ISOUSC,
           PEJP,
           PTEC,
           DEMAIN,
           ADPS,
           IMAX,
           HHPHC,
           MOTDETAT
    FROM stream
    ORDER BY id DESC LIMIT 1;

CREATE TRIGGER compute_stream_delta BEFORE INSERT ON stream
    FOR EACH ROW SET NEW.BASE_delta = NEW.BASE - (SELECT BASE FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.HCHC_delta = NEW.HCHC - (SELECT HCHC FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.HCHP_delta = NEW.HCHP - (SELECT HCHP FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.EJPHN_delta = NEW.EJPHN - (SELECT EJPHN FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.EJPHPM_delta = NEW.EJPHPM - (SELECT EJPHPM FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHCJB_delta = NEW.BBRHCJB - (SELECT BBRHCJB FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHPJB_delta = NEW.BBRHPJB - (SELECT BBRHPJB FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHCJW_delta = NEW.BBRHCJW - (SELECT BBRHCJW FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHPJW_delta = NEW.BBRHPJW - (SELECT BBRHPJW FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHCJR_delta = NEW.BBRHCJR - (SELECT BBRHCJR FROM stream ORDER BY id DESC LIMIT 1),
                     NEW.BBRHPJR_delta = NEW.BBRHPJR - (SELECT BBRHPJR FROM stream ORDER BY id DESC LIMIT 1);


delimiter |
CREATE EVENT IF NOT EXISTS daily_delta
    ON SCHEDULE
        -- start at midnight every day
        EVERY 1 DAY
        STARTS CURRENT_DATE + INTERVAL 1 DAY
    ON COMPLETION PRESERVE
    COMMENT "Calculate daily energy consumption"
    DO
        BEGIN
            INSERT INTO dailies (BASE_delta, HCHC_delta, HCHP_delta, EJPHN_delta, EJPHPM_delta, BBRHCJB_delta, BBRHPJB_delta, BBRHCJW_delta, BBRHPJW_delta, BBRHCJR_delta, BBRHPJR_delta)
                SELECT last.BASE-first.BASE,
                       last.HCHC-first.HCHC,
                       last.HCHP-first.HCHP,
                       last.EJPHN-first.EJPHN,
                       last.EJPHPM-first.EJPHPM,
                       last.BBRHCJB-first.BBRHCJB,
                       last.BBRHPJB-first.BBRHPJB,
                       last.BBRHCJW-first.BBRHCJW,
                       last.BBRHPJW-first.BBRHPJW,
                       last.BBRHCJR-first.BBRHCJR,
                       last.BBRHPJR-first.BBRHPJR
                FROM (
                    SELECT BASE, HCHC, HCHP, EJPHN, EJPHPM, BBRHCJB, BBRHPJB, BBRHCJW, BBRHPJW, BBRHCJR, BBRHPJR
                    FROM stream
                    WHERE `clock` >= NOW() - INTERVAL 1 DAY and `clock` < NOW()
                    ORDER BY id LIMIT 1
                ) as first, (
                    SELECT BASE, HCHC, HCHP, EJPHN, EJPHPM, BBRHCJB, BBRHPJB, BBRHCJW, BBRHPJW, BBRHCJR, BBRHPJR
                    FROM stream
                    WHERE `clock` >= NOW() - INTERVAL 1 DAY and `clock` < NOW()
                    ORDER BY id DESC LIMIT 1
                ) as last;
        END|
delimiter ;
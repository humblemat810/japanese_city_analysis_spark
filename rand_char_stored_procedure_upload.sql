USE JP_ADDRESS_ANALYSIS;
DELIMITER //
DROP PROCEDURE IF EXISTS rand_char ;
CREATE  PROCEDURE rand_char(OUT rand_char)
BEGIN
    SET @total_cnt = (SELECT sum(frequency) FROM JP_ADDRESS_WORD_FREQUENCIES);
    SELECT machi_jp_char 
    INTO rand_char 
    FROM JP_ADDRESS_WORD_FREQUENCIES 
    WHERE cumsum_frequency >= rand() * @total_cnt
    ORDER BY cumsum_frequency ASC
    LIMIT 1;
    
END //
DELIMITER ;
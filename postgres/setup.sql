CREATE EXTENSION intarray;

CREATE TYPE stats AS (
    id int4,
    wins int4,
    losses int4,
    draws int4
);

CREATE TYPE deckstats AS (
    id char(16),
    wins int4,
    losses int4,
    draws int4
);

CREATE TABLE gamecount (
    id varchar(255),
    count int4
);

/* for windows users */
CREATE OR REPLACE FUNCTION match_ba_deck(deck char, cards int4[])
RETURNS bool AS $$
DECLARE
    maxdptr int2;
    dptr int2 := 1;
    cptr int2 := 1;
BEGIN
    maxdptr := 17 - array_length(cards, 1) * 2;
    cards := sort(cards);

    WHILE maxdptr < 17 LOOP
        LOOP
            IF dptr > maxdptr THEN
                RETURN false;
            END IF;
            IF substring(deck from dptr for 2) = lpad(to_hex(cards[cptr]), 2, '0') THEN
                dptr := dptr + 2;
                EXIT;
            END IF;
            dptr := dptr + 2;
        END LOOP;

        cptr := cptr + 1;
        maxdptr := maxdptr + 2;
    END LOOP;

    RETURN true;
END;
$$ LANGUAGE plpgsql;

/* for based linux users who can actually compile the postgres c file */
CREATE FUNCTION match_ba_deck(char, int2[])
RETURNS bool
    AS '/var/lib/postgres/data/match_ba_deck.so', 'match_ba_deck'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION calc_ba_rating(wins int4, losses int4, draws int4)
RETURNS float8 AS $$
DECLARE
    total float8;
    z float8 := 3;
    z_squared float8 := 9;
    p float8;
    z_squared_over_total float8;
    avg float8;
    err float8;
    mul float8;
BEGIN
    total := wins + losses + draws;

    IF total = 0 THEN
        RETURN 0;
    END IF;
    
    p := (wins + 0.5 * draws::float8) / total;
    
    z_squared_over_total := z_squared / total;
    avg := p + 0.5 * z_squared_over_total;
    err := z * sqrt((p * (1 - p) + 0.25 * z_squared_over_total) / total);
    mul := 1 + z_squared_over_total;
    
    RETURN GREATEST(0, (avg - err) / mul);
END;
$$ LANGUAGE plpgsql;

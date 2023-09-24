#include <inttypes.h>
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"

PG_MODULE_MAGIC;

void i_sort(int16 *arr, uint8 len) {
    uint8 i, j;
    int16 t;

    for (i = 1; i < len; i++) {
        t = arr[i];
        for (j = i; j > 0 && arr[j - 1] > t; j--)
            arr[j] = arr[j - 1];
        arr[j] = t;
    }
}

PG_FUNCTION_INFO_V1(match_ba_deck);

Datum match_ba_deck(PG_FUNCTION_ARGS) {
    static char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    BpChar *arg0 = PG_GETARG_BPCHAR_PP(0);
    ArrayType *arg1 = PG_GETARG_ARRAYTYPE_P_COPY(1);
    
    char *deck = VARDATA(arg0);
    int16 *cards = (int16 *) ARR_DATA_PTR(arg1);
    uint8 ncards = ARR_DIMS(arg1)[0];
    
    i_sort(cards, ncards);
    
    uint8 dptr = 1;
    uint8 cptr = 0;
    while (ncards) {
        while (dptr <= 16 - (ncards << 1)) {
            dptr += 2;
            if (deck[dptr - 2] != hex[cards[cptr] >> 4] || deck[dptr - 1] != hex[cards[cptr] & 0xf])
                continue;
            ncards--;
            break;
        }

        if (dptr > 16 - (ncards << 1))
            PG_RETURN_BOOL(false);

        cptr++;
    }
    
    PG_RETURN_BOOL(true);
}

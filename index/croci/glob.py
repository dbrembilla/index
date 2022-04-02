#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from re import match

def normalise_multiple_ids(id_list, doi,metaid):
    normalised_ids=None
    for id in id_list:
        if match('10\..+\/.+$',id):
            # It's a DOI! So, normalise it as a doi. Put it in the normalised citing dict if it's normalisable.
            # If there are multiple dois, use the first one
            if not normalised_ids:
                id = doi.normalise(id)
                normalised_ids= {'doi':id}
            elif 'doi' not in normalised_citing:
                normalised_citing['doi'] = id

        elif match('^060|^br\/060|meta:br\/060'):
            if not normalised_citing:
                id = metaid.normalise(id)
                normalised_citing = {'metaid':id}
            elif 'metaid' not in normalised_citing:
                normalised_citing['metaid'] = id
    return normalised_ids
def re_classify_review_source(source, sub_source):
    source_site = source + '.com'
    all_parent_sources = ['bloomingdales.com', 'macys.com', 'nordstrom.com', 'ulta.com']
    classified_sub_source = None
    all_parent_sources.remove(source_site)
    allow_review = sub_source is None or sub_source not in all_parent_sources
    if allow_review:
        if sub_source:
            if 'spacenk.com' == sub_source:
                classified_sub_source = 'Space NK'
            elif 'bluemercury.com' == sub_source:
                classified_sub_source = 'bluemercury'
            elif 'influenster.com' == sub_source:
                classified_sub_source = 'influenster'
            else:
                classified_sub_source = 'BRAND.com'

    return allow_review, classified_sub_source

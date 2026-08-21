{#
    Creates a `raw_ipeds` schema of DuckDB views, one per IPEDS CSV in seeds/.

    This replaces the old Snowflake RAW_IPEDS tables: instead of loading the
    data into a warehouse, each view reads its CSV live via read_csv. The
    staging sources (_stg_sources.yml) point at these views, so the rest of
    the project is unchanged.

    nullstr handles IPEDS missing-value markers ('' and '.') so numeric
    columns are sniffed correctly and downstream ::numeric casts get NULL.
#}
{% macro create_raw_ipeds_views() %}
    {# IPEDS does not release every survey on the same schedule: completions
       (C_A), 12-month enrollment (EFFY) and the directory (HD) land about a
       year ahead of graduation rates (GR), retention (EF_D) and financial aid
       (SFA). Each family therefore carries its own end year -- the CSV has to
       exist in seeds/ or read_csv fails at run start. Bump a family's range
       once its next year is downloaded. #}
    {% set families = {
        'c_a':  range(2020, 2026),
        'ef_d': range(2020, 2025),
        'effy': range(2020, 2026),
        'gr':   range(2020, 2025),
        'hd':   range(2020, 2026),
        'sfa':  range(2020, 2025),
    } %}

    {# Path is relative to the dbt project dir; run dbt from inside texas_cc_benchmarking/. #}
    {% set seeds_dir = 'seeds' %}

    create schema if not exists raw_ipeds;

    {% for family, years in families.items() %}
        {% for year in years %}
            {% set table = family ~ '_' ~ year %}
            create or replace view raw_ipeds.{{ table }} as
                select * from read_csv(
                    '{{ seeds_dir }}/{{ table }}.csv',
                    header = true,
                    nullstr = ['', '.', 'NULL'],
                    sample_size = -1
                );
        {% endfor %}
    {% endfor %}
{% endmacro %}

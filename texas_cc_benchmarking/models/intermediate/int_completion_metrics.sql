-- int_completion_metrics.sql
-- Aggregate completions by institution for Texas community colleges

with completions as (
    select * from {{ ref('stg_ipeds__completions') }}
),

texas_ccs as (
    select unitid from {{ ref('int_texas_community_colleges') }}
),

-- Aggregate completions by institution and award level
institution_completions as (
    select
        c.unitid,

        -- Total completions across all award levels (cast to larger int)
        sum(c.total_completions)::number(10,0) as total_completions,

        -- Certificates < 1 year
        sum(case when c.award_level = 1 then c.total_completions else 0 end)::number(10,0) as certificates_under_1yr,

        -- Certificates 1-2 years
        sum(case when c.award_level = 2 then c.total_completions else 0 end)::number(10,0) as certificates_1_2yr,

        -- Associate degrees (PRIMARY metric for community colleges)
        sum(case when c.award_level = 3 then c.total_completions else 0 end)::number(10,0) as associate_degrees,

        -- All certificates combined
        sum(case when c.award_level in (1, 2) then c.total_completions else 0 end)::number(10,0) as total_certificates,

        -- Demographics for associate degrees (HB8 equity metrics)
        sum(case when c.award_level = 3 then c.hispanic_total else 0 end)::number(10,0) as associate_hispanic,
        sum(case when c.award_level = 3 then c.black_total else 0 end)::number(10,0) as associate_black,
        sum(case when c.award_level = 3 then c.white_total else 0 end)::number(10,0) as associate_white,
        sum(case when c.award_level = 3 then c.asian_total else 0 end)::number(10,0) as associate_asian,
        sum(case when c.award_level = 3 then c.amer_indian_total else 0 end)::number(10,0) as associate_amer_indian,
        sum(case when c.award_level = 3 then c.two_or_more_total else 0 end)::number(10,0) as associate_two_or_more,

        -- Gender breakdown for associate degrees
        sum(case when c.award_level = 3 then c.total_completions_men else 0 end)::number(10,0) as associate_men,
        sum(case when c.award_level = 3 then c.total_completions_women else 0 end)::number(10,0) as associate_women

    from completions c
    inner join texas_ccs t on c.unitid = t.unitid
    where c.cip_code = '99'  -- Grand total across all programs
    group by c.unitid
),

final as (
    select
        unitid,
        total_completions,
        certificates_under_1yr,
        certificates_1_2yr,
        total_certificates,
        associate_degrees,

        -- Demographic percentages for associate degrees
        round(associate_hispanic * 100.0 / nullif(associate_degrees, 0), 1) as pct_associate_hispanic,
        round(associate_black * 100.0 / nullif(associate_degrees, 0), 1) as pct_associate_black,
        round(associate_white * 100.0 / nullif(associate_degrees, 0), 1) as pct_associate_white,

        -- Gender percentages
        round(associate_men * 100.0 / nullif(associate_degrees, 0), 1) as pct_associate_men,
        round(associate_women * 100.0 / nullif(associate_degrees, 0), 1) as pct_associate_women,

        -- Raw counts for further analysis
        associate_hispanic,
        associate_black,
        associate_white,
        associate_asian,
        associate_amer_indian,
        associate_two_or_more,
        associate_men,
        associate_women

    from institution_completions
)

select * from final

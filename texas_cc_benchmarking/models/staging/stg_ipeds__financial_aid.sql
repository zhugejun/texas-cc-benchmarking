-- stg_ipeds__financial_aid.sql
-- financial aid by demographics


with source as (
    {{ dbt_utils.union_relations(
        relations=[
            source('raw_ipeds', 'sfa_2020'),
            source('raw_ipeds', 'sfa_2021'),
            source('raw_ipeds', 'sfa_2022'),
            source('raw_ipeds', 'sfa_2023'),
            source('raw_ipeds', 'sfa_2024'),
        ]
    ) }}
),

renamed as (
    select
        -- Identifiers
        unitid,
        year,  -- Added for multi-year support
        
        -- Undergraduate enrollment for financial aid cohort
        scugrad::number(10,0) as undergrad_enrollment_aid,
        scugdgsk::number(10,0) as undergrad_degree_seeking_count,
        scugndgs::number(10,0) as undergrad_non_degree_seeking_count,
        scugffn::number(10,0) as ftft_undergrad_count,  -- Full-time first-time (critical for HB8)
        scugffp as ftft_undergrad_percent,
        
        -- Fall cohort enrollment
        scfa2::number(10,0) as fall_cohort_total_undergrads,
        scfa2dg::number(10,0) as fall_cohort_degree_seeking,
        scfa2nd::number(10,0) as fall_cohort_non_degree_seeking,
        scfa1n::number(10,0) as fall_cohort_student_count,
        scfa1p as fall_cohort_percent_of_undergrads,
        
        -- Full-year cohort enrollment
        scfy2::number(10,0) as full_year_cohort_total_undergrads,
        scfy2dg::number(10,0) as full_year_cohort_degree_seeking,
        scfy2nd::number(10,0) as full_year_cohort_non_degree_seeking,
        scfy1n::number(10,0) as full_year_cohort_student_count,
        scfy1p as full_year_cohort_percent_of_undergrads,
        
        -- ALL UNDERGRADUATES: Pell Grant recipients (key for HB8 "economically disadvantaged")
        upgrntn::number(10,0) as all_ug_pell_recipients_count,
        upgrntp as all_ug_pell_recipients_percent,
        upgrntt::number(12,0) as all_ug_pell_total_amount,
        upgrnta::number(10,0) as all_ug_pell_avg_amount,
        
        -- ALL UNDERGRADUATES: Any grant aid (federal, state, local, or institutional)
        uagrntn::number(10,0) as all_ug_any_grant_recipients_count,
        uagrntp as all_ug_any_grant_recipients_percent,
        uagrntt::number(12,0) as all_ug_any_grant_total_amount,
        uagrnta::number(10,0) as all_ug_any_grant_avg_amount,
        
        -- ALL UNDERGRADUATES: Federal student loans
        ufloann::number(10,0) as all_ug_federal_loan_recipients_count,
        ufloanp as all_ug_federal_loan_recipients_percent,
        ufloant::number(12,0) as all_ug_federal_loan_total_amount,
        ufloana::number(10,0) as all_ug_federal_loan_avg_amount,
        
        -- DEGREE-SEEKING UNDERGRADUATES: Pell grants (important for HB8 degree completion metrics)
        udgpgrntn::number(10,0) as degree_seeking_pell_recipients_count,
        udgpgrntp as degree_seeking_pell_recipients_percent,
        udgpgrntt::number(12,0) as degree_seeking_pell_total_amount,
        udgpgrnta::number(10,0) as degree_seeking_pell_avg_amount,
        
        -- DEGREE-SEEKING UNDERGRADUATES: Any grant aid
        udgagrntn::number(10,0) as degree_seeking_any_grant_recipients_count,
        udgagrntp as degree_seeking_any_grant_recipients_percent,
        udgagrntt::number(12,0) as degree_seeking_any_grant_total_amount,
        udgagrnta::number(10,0) as degree_seeking_any_grant_avg_amount,
        
        -- DEGREE-SEEKING UNDERGRADUATES: Federal loans
        udgfloann::number(10,0) as degree_seeking_federal_loan_recipients_count,
        udgfloanp as degree_seeking_federal_loan_recipients_percent,
        udgfloant::number(12,0) as degree_seeking_federal_loan_total_amount,
        udgfloana::number(10,0) as degree_seeking_federal_loan_avg_amount,
        
        -- FULL-TIME FIRST-TIME (FTFT): Any financial aid
        anyaidn::number(10,0) as ftft_any_aid_recipients_count,
        anyaidp as ftft_any_aid_recipients_percent,
        aidfsin::number(10,0) as ftft_loan_or_grant_recipients_count,
        aidfsip as ftft_loan_or_grant_recipients_percent,
        
        -- FTFT: Federal, state, local, or institutional grants combined
        agrnt_n::number(10,0) as ftft_any_grant_recipients_count,
        agrnt_p as ftft_any_grant_recipients_percent,
        agrnt_t::number(12,0) as ftft_any_grant_total_amount,
        agrnt_a::number(10,0) as ftft_any_grant_avg_amount,
        
        -- FTFT: Federal grants
        fgrnt_n::number(10,0) as ftft_federal_grant_recipients_count,
        fgrnt_p as ftft_federal_grant_recipients_percent,
        fgrnt_t::number(12,0) as ftft_federal_grant_total_amount,
        fgrnt_a::number(10,0) as ftft_federal_grant_avg_amount,
        
        -- FTFT: Pell grants (critical for HB8)
        pgrnt_n::number(10,0) as ftft_pell_recipients_count,
        pgrnt_p as ftft_pell_recipients_percent,
        pgrnt_t::number(12,0) as ftft_pell_total_amount,
        pgrnt_a::number(10,0) as ftft_pell_avg_amount,
        
        -- FTFT: Other federal grants (non-Pell)
        ofgrt_n::number(10,0) as ftft_other_federal_grant_recipients_count,
        ofgrt_p as ftft_other_federal_grant_recipients_percent,
        ofgrt_t::number(12,0) as ftft_other_federal_grant_total_amount,
        ofgrt_a::number(10,0) as ftft_other_federal_grant_avg_amount,
        
        -- FTFT: State/local grants
        sgrnt_n::number(10,0) as ftft_state_local_grant_recipients_count,
        sgrnt_p as ftft_state_local_grant_recipients_percent,
        sgrnt_t::number(12,0) as ftft_state_local_grant_total_amount,
        sgrnt_a::number(10,0) as ftft_state_local_grant_avg_amount,
        
        -- FTFT: Institutional grants
        igrnt_n::number(10,0) as ftft_institutional_grant_recipients_count,
        igrnt_p as ftft_institutional_grant_recipients_percent,
        igrnt_t::number(12,0) as ftft_institutional_grant_total_amount,
        igrnt_a::number(10,0) as ftft_institutional_grant_avg_amount,
        
        -- FTFT: Student loans
        loan_n::number(10,0) as ftft_loan_recipients_count,
        loan_p as ftft_loan_recipients_percent,
        loan_t::number(12,0) as ftft_loan_total_amount,
        loan_a::number(10,0) as ftft_loan_avg_amount,
        
        -- FTFT: Federal student loans
        floan_n::number(10,0) as ftft_federal_loan_recipients_count,
        floan_p as ftft_federal_loan_recipients_percent,
        floan_t::number(12,0) as ftft_federal_loan_total_amount,
        floan_a::number(10,0) as ftft_federal_loan_avg_amount,
        
        -- FTFT: Other (non-federal) student loans
        oloan_n::number(10,0) as ftft_other_loan_recipients_count,
        oloan_p as ftft_other_loan_recipients_percent,
        oloan_t::number(12,0) as ftft_other_loan_total_amount,
        oloan_a::number(10,0) as ftft_other_loan_avg_amount

    from source
)



select * from renamed
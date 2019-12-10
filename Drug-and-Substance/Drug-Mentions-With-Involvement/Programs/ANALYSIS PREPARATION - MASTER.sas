/***************************************************************************************************************
PROGRAM NAME:       ANALYSIS PREPARATION - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro ANALYSIS_PREPARATION(MENTIONS_ONLY=,
							DEMOGRAPHICS=
							);

	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

    OPTIONS AUTOSIGNON;

    %put date ANALYSIS_PREPARATION macro started %sysfunc(date(),worddate.);
    %put time ANALYSIS_PREPARATION macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &MENTIONS_ONLY 			~= 	%then %let MENTIONS_ONLY 	=	%sysfunc(compress(&MENTIONS_ONLY));
	%if &DEMOGRAPHICS 			~= 	%then %let DEMOGRAPHICS 	=	%sysfunc(compress(&DEMOGRAPHICS));

	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

	%put date ANALYSIS_PREPARATION_SIGN_ON START %sysfunc(date(),worddate.);
    %put time ANALYSIS_PREPARATION_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

	OPTIONS AUTOSIGNON;
	%LET RCAP=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCAP;
	signon cspAP wait=yes MACVAR=SIGNON_RC ;
		%log_signon(session_name=cspAP);

	    %SYSLPUT year_start            		= &year_start              		/remote=cspAP;
	    %SYSLPUT year_end              		= &year_end               		/remote=cspAP;
	    %SYSLPUT log_folder              	= &log_folder              		/remote=cspAP;
	    %SYSLPUT MENTIONS_ONLY              = &MENTIONS_ONLY              	/remote=cspAP;
	    %SYSLPUT DEMOGRAPHICS              	= &DEMOGRAPHICS              	/remote=cspAP;

    rsubmit cspAP wait=yes inheritlib=(data input raw results temp);
		proc Printto NEW LOG="&LOG_FOLDER.AP_BODY.txt";

	%put date ANALYSIS_PREPARATION_SIGN_ON DONE %sysfunc(date(),worddate.);
	%put time ANALYSIS_PREPARATION_SIGN_ON DONE %sysfunc(time(),time8.2);

	%macro analysisPrep();
		%do year = &year_start %to &year_end;
			proc sql;
				create table results.mention_level_results_&year (compress = yes) as
				select distinct *
					from data.cleaned_records_&year
						%if %sysfunc(exist(data.mentions_&year)) and &MENTIONS_ONLY = 1 %then %do;
							where uniq_ID in
								(select uniq_ID 
									from data.mentions_&year
								)
						%end;
				;
				%if %sysfunc(exist(data.selected_population_&year)) and &DEMOGRAPHICS = 1 %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select distinct *
								from data.selected_population_&year
							) as b&year
							on a&year..uniq_ID = b&year..uniq_ID
					;
				%end;
				%else %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select distinct uniq_ID, stocc, certno
								from data.selected_population_&year
							) as b&year
							on a&year..uniq_ID = b&year..uniq_ID
					;
				%end;
				%if %sysfunc(exist(data.mentions_&year)) %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select distinct uniq_ID,
									search_term,
									text_field,
									term_position_beg,
									term_position_end
								from data.mentions_&year
							) as c&year
							on a&year..uniq_ID = c&year..uniq_ID
					;
				%end;
				%if %sysfunc(exist(data.mentions_&year)) and %sysfunc(exist(input.general_search_terms)) %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select *
								from input.general_search_terms
							) as general_search_terms
							on 	(a&year..search_term = general_search_terms.search_term) or
								(a&year..search_term = cats(general_search_terms.search_term,"S"))
					;
				%end;
				%if %sysfunc(exist(data.mentions_&year)) and %sysfunc(exist(input.user_defined_search_terms)) %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select *
								from input.user_defined_search_terms
							) as user_defined_search_terms
							on 	(a&year..search_term = user_defined_search_terms.search_term) or
								(a&year..search_term = cats(user_defined_search_terms.search_term,"S"))
					;
				%end;
				%if %sysfunc(exist(data.phrase_mentions_&year)) %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select distinct uniq_ID,
									text_field,
									term_position_beg,
									term_position_end,
									pre_qualifier,
									post_qualifier,
									distilled_position,
									phrase_beg,
									phrase_end,
									phrase
								from data.phrase_mentions_&year
							) as d&year
							on 	a&year..uniq_ID = d&year..uniq_ID and
								a&year..text_field = d&year..text_field and
								a&year..term_position_beg = d&year..term_position_beg
					;
				%end;
				%if %sysfunc(exist(data.phrase_mentions_&year)) and %sysfunc(exist(input.phrases)) %then %do;
					create table results.mention_level_results_&year (compress = yes) as
					select distinct *
						from
							(select *
								from results.mention_level_results_&year
							) as a&year
							left join
							(select *
								from input.phrases (drop=phrase_list)
							) as input_phrases
							on 	a&year..phrase = input_phrases.phrase
					;
				%end;
			quit;






/*				create table results.mention_level_results_&year (compress = yes) as*/
/*				select distinct **/
/*					from*/
/*						(select distinct **/
/*							from data.cleaned_records_&year*/
/*								%if %sysfunc(exist(data.mentions_&year)) and &MENTIONS_ONLY = 1 %then %do;*/
/*									where uniq_ID in*/
/*										(select uniq_ID */
/*											from data.mentions_&year*/
/*										)*/
/*								%end;*/
/*							) as a&year*/
/*							%if %sysfunc(exist(data.selected_population_&year)) and &DEMOGRAPHICS = 1 %then %do;*/
/*								left join*/
/*								(select distinct **/
/*									from data.selected_population_&year*/
/*										%if %sysfunc(exist(data.mentions_&year)) and &MENTIONS_ONLY = 1 %then %do;*/
/*											where uniq_ID in*/
/*												(select uniq_ID */
/*													from data.mentions_&year*/
/*												)*/
/*										%end;*/
/*								) as b&year*/
/*								on a&year..uniq_ID = b&year..uniq_ID*/
/*							%end;*/
/*							%else %do;*/
/*								left join*/
/*								(select distinct uniq_ID, stocc, certno*/
/*									from data.selected_population_&year*/
/*										%if %sysfunc(exist(data.mentions_&year)) and &MENTIONS_ONLY = 1 %then %do;*/
/*											where uniq_ID in*/
/*												(select uniq_ID */
/*													from data.mentions_&year*/
/*												)*/
/*										%end;*/
/*								) as b&year*/
/*								on a&year..uniq_ID = b&year..uniq_ID*/
/*							%end;*/
/*							%if %sysfunc(exist(data.mentions_&year)) %then %do;*/
/*								left join*/
/*								(select distinct uniq_ID,*/
/*										search_term,*/
/*										text_field,*/
/*										term_position_beg,*/
/*										term_position_end*/
/*									from data.mentions_&year*/
/*								) as c&year*/
/*								on a&year..uniq_ID = c&year..uniq_ID*/
/*								%if %sysfunc(exist(input.general_search_terms)) %then %do;*/
/*									left join*/
/*									(select **/
/*										from input.general_search_terms*/
/*									) as general_search_terms*/
/*									on 	(c&year..search_term = general_search_terms.search_term) or*/
/*										(c&year..search_term = cats(general_search_terms.search_term,"S"))*/
/*								%end;*/
/*								%if %sysfunc(exist(input.user_defined_search_terms)) %then %do;*/
/*									left join*/
/*									(select **/
/*										from input.user_defined_search_terms*/
/*									) as user_defined_search_terms*/
/*									on 	(c&year..search_term = user_defined_search_terms.search_term) or*/
/*										(c&year..search_term = cats(user_defined_search_terms.search_term,"S"))*/
/*								%end;*/
/*							%end;*/

/*							%if %sysfunc(exist(temp.qualifiers_2013)) %then %do;*/
/*								left join*/
/*								(select **/
/*									from*/
/*										(select **/
/*											from*/
/*												(select **/
/*													from temp.qualifiers_2013*/
/*													where pre_qualifier ~= ""*/
/*												) as temp_prequalifiers*/
/*												left join*/
/*												(select **/
/*													from input.qualifiers*/
/*													where pre_qualifier ~= ""*/
/*												) as pre_qualifiers*/
/*												on 	(temp_prequalifiers.pre_qualifier = pre_qualifiers.pre_qualifier)*/
/*										)*/
/*										union*/
/*										(select **/
/*											from*/
/*												(select **/
/*													from temp.qualifiers_2013*/
/*													where post_qualifier ~= ""*/
/*												) as temp_postqualifiers*/
/*												left join*/
/*												(select **/
/*													from input.qualifiers*/
/*													where post_qualifier ~= ""*/
/*												) as post_qualifiers*/
/*												on 	(temp_postqualifiers.post_qualifier = post_qualifiers.post_qualifier)*/
/*										)*/
/*								) as qualifiers*/
/*								on 	(c2013.uniq_ID = qualifiers.uniq_ID) and*/
/*									(c2013.text_field = qualifiers.text_field) and*/
/*									(c2013.term_position_beg = qualifiers.term_position_beg)*/
/*							%end;*/

/*							%if %sysfunc(exist(data.phrase_mentions_&year)) %then %do;*/
/*								left join*/
/*								(select distinct uniq_ID,*/
/*										text_field,*/
/*										term_position_beg,*/
/*										term_position_end,*/
/*										distilled_position,*/
/*										phrase_beg,*/
/*										phrase_end,*/
/*										phrase*/
/*									from data.phrase_mentions_&year*/
/*								) as d&year*/
/*								on 	c&year..uniq_ID = d&year..uniq_ID and*/
/*									c&year..text_field = d&year..text_field and*/
/*									c&year..term_position_beg = d&year..term_position_beg*/
/*								%if %sysfunc(exist(input.phrases)) %then %do;*/
/*									left join*/
/*									(select **/
/*										from input.phrases (drop=phrase_list)*/
/*									) as input_phrases*/
/*									on 	d&year..phrase = input_phrases.phrase*/
/*								%end;*/
/*							%end;*/
/*							order by 	uniq_ID,*/
/*										text_field,*/
/*										term_position_beg*/
/*				;*/
/*			quit;*/
		%end;
	%mend analysisPrep;

	%analysisPrep();

	endrsubmit;
	signoff cspAP;

    %put date ANALYSIS_PREPARATION macro ended %sysfunc(date(),worddate.);
    %put time ANALYSIS_PREPARATION macro ended %sysfunc(time(),time5.0);
%mend;

/***************************************************************************************************************
PROGRAM NAME:       SELECT POPULATION - MASTER
VERSION:			CSP Update 2016 April 01
****************************************************************************************************************/

%macro POPULATION_SELECTION_MASTER( INPUT_DATASET_FILE=,
							 FIRSTOBS=, 
                             OBS=, 
							 STRES=,
							 CSP_OPTIONS=CSP_MOD							
                            );
	/***********************************************************************************************************
	                                                SECTION 1
	************************************************************************************************************/

    %put date POPULATION_SELECTION macro started %sysfunc(date(),worddate.);
    %put time POPULATION_SELECTION macro started %sysfunc(time(),time5.0);

	%let year_start				= %sysfunc(compress(&year_start))		;
	%let year_end				= %sysfunc(compress(&year_end))		 	;
	%let programpath			= %sysfunc(compress(%str(&programpath)));
	%let projectpath			= %sysfunc(compress(%str(&projectpath)));
	%let LOG_FOLDER				= %sysfunc(compress(%str(&LOG_FOLDER)))	;
	%let autoexec				= %sysfunc(compress(%str(&autoexec_folder)))	;

	%if &INPUT_DATASET_FILE 	~= %then %let INPUT_DATASET_FILE		=%sysfunc(compress(&INPUT_DATASET_FILE))	;
	%if &FIRSTOBS 				~= %then %let FIRSTOBS 					=%sysfunc(compress(&FIRSTOBS)) 				;
	%if &OBS 					~= %then %let OBS						=%sysfunc(compress(&OBS))					;
	%if &STRES  				~= %then %let STRES						=%sysfunc(compress(&STRES))				;
	/***********************************************************************************************************
	                                                SECTION 2
	************************************************************************************************************/

    %do year = &year_start %to &year_end;
		/*INPUT_DATASET_FILE and INPUT_DATASET_FOLDER*/
		%if &INPUT_DATASET_FILE 	= selected_population %then %do;
			%let INPUT_DATASET_FOLDER 	= data;
			%put NOTE: Records to be processed will come from &INPUT_DATASET_FOLDER folder.;
			%put NOTE: Records to be processed will come from the %str(selected_population_&year) file.;
		%end;

		%else %do;
			%let INPUT_DATASET_FOLDER 	= raw;
			%let INPUT_DATASET_FILE		= histmor_smicar_innerj;
			%put NOTE: Records to be processed will come from &INPUT_DATASET_FOLDER folder.;
			%put NOTE: Records to be processed will come from the %str(histmor_smicar_innerj_&year) file.;
		%end;
	%end;

	/*firstobs_year*/
	/*obs_year*/
	/*Check INPUT_DATASET_FOLDER.INPUT_DATASET_FILE*/
    %do year = &year_start %to &year_end;
        %let firstobs_&year = 1;

        %let check_file = %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));

        %if &check_file = 1 %then %do;
            %let obs_&year  = %sysfunc(attrn(&check_file,nlobs));
            %let check_file = %sysfunc(close(1)); 
        %end;

        %else %do;
            %put WARNING: &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year does not exist;
			%let INPUT_DATASET_FOLDER 	= raw;
			%let INPUT_DATASET_FILE 	= histmor_smicar_innerj;
            %put NOTE: File containing records to be processed will be the %str(raw.histmor_smicar_innerj_&year) file.;

	        %let check_file = %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));

	        %if &check_file = 1 %then %do;
	            %let obs_&year  = %sysfunc(attrn(&check_file,nlobs));
	            %let check_file = %sysfunc(close(1)); 
	        %end;
        %end;
	    %put NOTE: For year &year, input dataset contains &&obs_&year. observations;
		%let old_obs_&year = &&obs_&year;
    %end;


    %do year = &year_start %to &year_end;
		/*FIRSTOBS -> first_obs_year*/
        %if &FIRSTOBS = %then %do;
            %let firstobs_&year = 1;
            %put NOTE: For year &year, the first record read from the input dataset will be record 1.;
        %end;

        %else %do;
            %let firstobs_&year = &FIRSTOBS;
			%put NOTE: For year &year, the first record read from the input dataset will be record &&firstobs_&year.; 
        %end;

		/*OBS -> obs_year*/
        %if &OBS = %then %do;
            %let dsid       =   %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));
            %let obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
            %let rc         =   %sysfunc(close(&dsid));
            %put NOTE: For year &year, the last record read from the input dataset will be record &&obs_&year.;
        %end;

        %else %do;
            %let dsid       =   %sysfunc(open(raw.histmor_smicar_innerj_&year));
            %let obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
            %let rc         =   %sysfunc(close(&dsid));
			%if  &&obs_&year < &obs %then %let &obs = obs_&year;
			%let obs_&year 	= &obs;
            %put NOTE: For year &year, the last record read from the input dataset will be record &&obs_&year.;
        %end;

		/*firstobs_year and obs_year check*/
        %if &&obs_&year < &&firstobs_&year %then %do;
            %let firstobs_&year = 1;
            %put NOTE: FIRSTOBS > OBS.  For year &year, the first record read from input dataset will be record 1.;
        %end;
	%end;

	/***********************************************************************************************************
	                                                SECTION 3
	************************************************************************************************************/

	%put date POPULATION_SELECTION_SIGN_ON START %sysfunc(date(),worddate.);
    %put time POPULATION_SELECTION_SIGN_ON START %sysfunc(time(),time8.2);

	%include "\\cdc\csp_Project\CIPSEA_PII_DVS_MSB\Drug_Involved_Mortality\Final_Programs\2019-01-18\log_csp_signon.sas" /NOSOURCE2 ;
	%initialize_signon();

	OPTIONS AUTOSIGNON;
   	%LET RCPS=%SYSFUNC(grdsvc_enable(_ALL_,resource=CSP_MNL )) ;
   	%PUT Return Code = &RCPS ;
	signon cspPS wait=yes MACVAR=SIGNON_RC;
		%log_signon(session_name=cspPS);

        %SYSLPUT year_start      	= &year_start         		/remote=cspPS;
        %SYSLPUT year_end        	= &year_end           		/remote=cspPS;
        %SYSLPUT LOG_FOLDER        	= &LOG_FOLDER          		/remote=cspPS;

		%SYSLPUT INPUT_DATASET_FOLDER = &INPUT_DATASET_FOLDER 	/remote=cspPS;
        %SYSLPUT INPUT_DATASET_FILE = &INPUT_DATASET_FILE   	/remote=cspPS;
		%do year = &year_start %to &year_end;
			%SYSLPUT obs_&year		= &&obs_&year        		/remote=cspPS;
			%SYSLPUT firstobs_&year = &&firstobs_&year   		/remote=cspPS;
			%put obs_&year = &&obs_&year;
			%put firstobs_&year = &&firstobs_&year;
		%end;
	    %SYSLPUT STRES        		= &STRES          			/remote=cspPS;

        rsubmit cspPS wait=yes inheritlib=(data input raw results temp);
			proc Printto NEW LOG="&LOG_FOLDER.PS_BODY.txt";

	%put date POPULATION_SELECTION_SIGN_ON DONE %sysfunc(date(),worddate.);
	%put time POPULATION_SELECTION_SIGN_ON DONE %sysfunc(time(),time8.2);

			%macro populationSelect();
			    %do year = &year_start %to &year_end;
					proc sql;
						create table data.selected_population_&year (compress = yes) as
							select *
							from &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year
			                    (firstobs	= &&firstobs_&year
				                    obs 	= &&obs_&year
			                    )               
							where uniq_ID ~= ""
							%if &stres ~= %then %do;
								%if &stres = US_Residents %then %do;
									and stres in 
											('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
											'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
											'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
											'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
											'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI',
											'WY', 'YC'
											)
								%end;
							%end;
							;
					quit;
			    %end;

	/***********************************************************************************************************
	                                                SECTION 4
	************************************************************************************************************/

			%mend populationSelect;

			%populationSelect();

		endrsubmit;
 	signoff cspPS;

	/*Note: Observations in input dataset &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year is old_obs_&year*/
/*    %do year = &year_start %to &year_end;*/
/*        %let dsid           =   %sysfunc(open(&INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year));*/
/*        %let obs_&year  	=   %sysfunc(attrn(&dsid,nlobs));*/
/*        %let rc             =   %sysfunc(close(&dsid));*/
/*    %end;*/

    %do year = &year_start %to &year_end;
        %let dsid           =   %sysfunc(open(data.selected_population_&year));
        %let new_obs_&year  =   %sysfunc(attrn(&dsid,nlobs));
        %let rc             =   %sysfunc(close(&dsid));
    %end;

    data check_data;
        length log_text $256.;
        %do year = &year_start %to &year_end;
            log_text = "observations in input dataset &INPUT_DATASET_FOLDER..&INPUT_DATASET_FILE._&year = &&old_obs_&year";
            output;
            log_text = "observations in output dataset data.selected_population_&year = &&new_obs_&year";
            output;
        %end;
    run;

    data work.check_all;
        set work.check_data (keep = log_text)
        ;
    run;

    title "check for errors";
        proc print 
            data=work.check_all noobs; 
        run;
    title;

    %put date POPULATION_SELECTION macro ended %sysfunc(date(),worddate.);
    %put time POPULATION_SELECTION macro ended %sysfunc(time(),time5.0);
%mend;


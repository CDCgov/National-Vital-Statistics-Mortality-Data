************************************************************************************ ;
* SIGNON TO CSP GRID                                                               * ;
*                                                                                  * ;
* J_Brittain (ZQR0), E_Call (VRA6), S_Srinivasan (WSW9)                            * ;
*                                                                                  * ;
* 2015-02-14 - Signon to the selected CSP GRID                                     * ;
*            - Log details including Errors and Time (SAS/SHARE used)              * ;
*            - Provide a screen if login fails and STOR SAS processing             * ;
*            - INPUT PARAMETERS: RESOURCE    =(CSP_MNL | CSP_LNL)                  * ;
*                                SESSION_NAME=(CSP1 | user_selected_name)          * ;
*                                DETAILS     =(NO | YES) SAS LOG SOURCE NOTES      * ;
************************************************************************************ ;
%macro initialize_signon(RESOURCE    =CSP_MOD ,
						 DETAILS     =NO);
  %global STARTTIME
  		  grdsvc_rc
		  ;

  data _null_ ;
    call symput('CURTIME',put(time(),TIME.)) ;
    call symput('STARTTIME',put(time(),11.)) ;
  run ;

  %PUT NOTE: *************************************************** ;   
  %PUT NOTE: * SIGNING ON TO THE CSP GRID:  RESOURCE=&RESOURCE. ;   
  %PUT NOTE: *                              START TIME= &CURTIME. ;
  %PUT NOTE: *************************************************** ; 

  %LET grdsvc_rc = %SYSFUNC(grdsvc_enable(_ALL_,resource=&RESOURCE)) ;
  %PUT NOTE: GRDSVC_ENABLE Return Code = &GRDSVC_RC. ;

%mend initialize_signon;

%MACRO log_signon (RESOURCE    =CSP_MOD ,
                   SESSION_NAME=slave
                  ) ;
  %local GRIDHOST
         CSPUSER 
         CURTIME  
         STARTUP_TIME
         DATE 
         CSPVDI 
         ENDTIME
         ;

  %PUT NOTE: SIGNON Return Code = &SIGNON_RC. ;

  data _null_ ;
    call symput('DATE',put(today(),MMDDYY10.)) ;
    call symput('CURTIME',put(time(),TIMEAMPM8.)) ;
    call symput('ENDTIME',put(time(),11.)) ;
    call symput('CSPVDI',"%sysget(computername)") ;  
    call symput('CSPUSER',upcase("%sysget(username)")) ;
  run ;

  data _null_ ;
    call symput('STARTUP_TIME',put(input("&ENDTIME.",11.)-input("&STARTTIME.",11.),MMSS.)) ;
  run ;

  %PUT NOTE: ********************************************************************* ;   
  %PUT NOTE: * TRIED TO SIGN ON TO A GRID SESSION:  RESOURCE=&RESOURCE. ; 
  %PUT NOTE: * 										SESSION=&SESSION_NAME ;
  %PUT NOTE: * 										END TIME= &CURTIME. ;
  %PUT NOTE: ********************************************************************* ; 

  rsubmit ;
    %MACRO GET_GRIDHOST ;
      data _null_ ;
        call symput('GRIDHOST',"%sysget(computername)") ;  
      run ;

      %SYSRPUT GRIDHOST = &GRIDHOST. ;
	%MEND ;
    %GET_GRIDHOST ;
  endrsubmit ;


/*  data new_record ;*/
/*    length signon_date      4*/
/*           signon_time      6*/
/*           user_id        $ 8*/
/*  		   csp_vdi        $10*/
/*		   sas_grid       $12*/
/*		   startup_time     6*/
/*		   resource       $ 8*/
/*		   session_name   $ 4*/
/*		   grdsvc_rc        3*/
/*		   signon_rc        3*/
/*           ;*/
/*    format signon_date  mmddyy10. */
/*           signon_time  hhmm6.*/
/*           startup_time mmss5.*/
/*           ;*/
/*      signon_date   = input("&DATE.",mmddyy10.) ;*/
/*      signon_time   = input("&CURTIME.",TIME8.0) ;*/
/*      user_id       = "&CSPUSER." ;*/
/*  	  csp_vdi       = "&CSPVDI." ;*/
/*	  sas_grid      = "&GRIDHOST." ;*/
/*	  startup_time  = input("00:0"||left("&STARTUP_TIME."),TIME8.2) ;*/
/*	  resource      = "&RESOURCE." ;*/
/*	  session_name  = substr("&SESSION_NAME",length("&SESSION_NAME")-3,4);*/
/*	  grdsvc_rc     = input("&GRDSVC_RC.",6.) ;*/
/*	  signon_rc     = input("&SIGNON_RC.",1.) ;*/
/*  run ;*/

  * SAS/SHARE LIBNAME * ;
/*  libname logging remote */
/*          hostname='app-v-csp2.cdc.gov' server=__8551 ;*/
/**/
/*  proc append base=logging.signon_log */
/*              data=new_record ;*/
/*  run ;*/
/*  proc delete data=new_record ;*/
/*  run ;*/

/*  libname logging clear ; */
  %IF (&GRDSVC_RC.=0 & &SIGNON_RC.=0) %THEN %DO ;  
    %PUT NOTE: ********************************************************************* ;   
    %PUT NOTE: * &DATE. - &CURTIME.  USER: &CSPUSER. ON: &CSPVDI.                    ;
    %PUT NOTE: *              STARTED A CSP SAS/GRID SESSION WITH: &GRIDHOST.        ;
    %PUT NOTE: *              STARTUP TOOK: &STARTUP_TIME.                           ;
    %PUT NOTE: ********************************************************************* ;

  %END ;
  %ELSE %IF (&GRDSVC_RC.^=0) %THEN %DO ;   

  	%PUT ERROR: ********************************************************************* ;
	%PUT ERROR: * GRDSVC_ENABLE call Failed                                           ;
	%PUT ERROR: * Could not successfully authenticate to the CSP metadata server      ;
	%PUT ERROR: *   (Ensure that your password on the CSP Grid is current)            ;
    %PUT ERROR: *   SAS PROCESSING HAS ABORTED                                        ;
	%PUT ERROR: ********************************************************************* ;

  %END ; 
  %ELSE %IF (&SIGNON_RC.=1) %THEN %DO ;

  	%PUT ERROR: ********************************************************************* ;
	%PUT ERROR: * GRDSVC_ENABLE call Failed                                           ;
	%PUT ERROR: * Could not successfully authenticate to the CSP metadata server      ;
	%PUT ERROR: *   (Ensure that your password on the CSP Grid is current)            ;
    %PUT ERROR: *   SAS PROCESSING HAS ABORTED                                        ;
	%PUT ERROR: ********************************************************************* ;

  %END ;
  %ELSE %IF (&SIGNON_RC.=2) %THEN %DO ;

    %PUT NOTE: ********************************************************************* ;
    %PUT NOTE: * SignOn command failed because you are already signed on.            ;
    %PUT NOTE: *   (SAS Processing will continue)                                    ;
    %PUT NOTE: ********************************************************************* ;

  %END ; 
  %ELSE %IF (&SIGNON_RC.=3) %THEN %DO ;

    %PUT ERROR: ********************************************************************* ;
    %PUT ERROR: * SignOn command failed because there is another signon in progress   ;
    %PUT ERROR: *   SAS PROCESSING HAS ABORTED                                        ;
    %PUT ERROR: ********************************************************************* ;

  %END ;

%MEND ;

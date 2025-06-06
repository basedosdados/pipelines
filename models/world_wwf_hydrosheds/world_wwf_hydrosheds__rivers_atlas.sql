{{
    config(
        alias="rivers_atlas",
        schema="world_wwf_hydrosheds",
        materialized="table",
        cluster_by="region",
    )
}}
select
    safe_cast(region as string) region,
    safe_cast(hyriv_id as int64) hyriv_id,
    safe_cast(next_down as int64) next_down,
    safe_cast(main_riv as int64) main_riv,
    safe_cast(length_km as float64) length_km,
    safe_cast(dist_dn_km as float64) dist_dn_km,
    safe_cast(dist_up_km as float64) dist_up_km,
    safe_cast(catch_skm as float64) catch_skm,
    safe_cast(upland_skm as float64) upland_skm,
    safe_cast(endorheic as int64) endorheic,
    safe_cast(dis_av_cms as float64) dis_av_cms,
    safe_cast(ord_stra as int64) ord_stra,
    safe_cast(ord_clas as int64) ord_clas,
    safe_cast(ord_flow as int64) ord_flow,
    safe_cast(hybas_l12 as int64) hybas_l12,
    safe_cast(dis_m3_pyr as float64) dis_m3_pyr,
    safe_cast(dis_m3_pmn as float64) dis_m3_pmn,
    safe_cast(dis_m3_pmx as float64) dis_m3_pmx,
    safe_cast(run_mm_cyr as int64) run_mm_cyr,
    safe_cast(inu_pc_cmn as int64) inu_pc_cmn,
    safe_cast(inu_pc_umn as int64) inu_pc_umn,
    safe_cast(inu_pc_cmx as int64) inu_pc_cmx,
    safe_cast(inu_pc_umx as int64) inu_pc_umx,
    safe_cast(inu_pc_clt as int64) inu_pc_clt,
    safe_cast(inu_pc_ult as int64) inu_pc_ult,
    safe_cast(lka_pc_cse as int64) lka_pc_cse,
    safe_cast(lka_pc_use as int64) lka_pc_use,
    safe_cast(lkv_mc_usu as int64) lkv_mc_usu,
    safe_cast(rev_mc_usu as int64) rev_mc_usu,
    safe_cast(dor_pc_pva as int64) dor_pc_pva,
    safe_cast(ria_ha_csu as float64) ria_ha_csu,
    safe_cast(ria_ha_usu as float64) ria_ha_usu,
    safe_cast(riv_tc_csu as float64) riv_tc_csu,
    safe_cast(riv_tc_usu as float64) riv_tc_usu,
    safe_cast(gwt_cm_cav as int64) gwt_cm_cav,
    safe_cast(ele_mt_cav as int64) ele_mt_cav,
    safe_cast(ele_mt_uav as int64) ele_mt_uav,
    safe_cast(ele_mt_cmn as int64) ele_mt_cmn,
    safe_cast(ele_mt_cmx as int64) ele_mt_cmx,
    safe_cast(slp_dg_cav as int64) slp_dg_cav,
    safe_cast(slp_dg_uav as int64) slp_dg_uav,
    safe_cast(sgr_dk_rav as int64) sgr_dk_rav,
    safe_cast(clz_cl_cmj as int64) clz_cl_cmj,
    safe_cast(cls_cl_cmj as int64) cls_cl_cmj,
    safe_cast(tmp_dc_cyr as int64) tmp_dc_cyr,
    safe_cast(tmp_dc_uyr as int64) tmp_dc_uyr,
    safe_cast(tmp_dc_cmn as int64) tmp_dc_cmn,
    safe_cast(tmp_dc_cmx as int64) tmp_dc_cmx,
    safe_cast(tmp_dc_c01 as int64) tmp_dc_c01,
    safe_cast(tmp_dc_c02 as int64) tmp_dc_c02,
    safe_cast(tmp_dc_c03 as int64) tmp_dc_c03,
    safe_cast(tmp_dc_c04 as int64) tmp_dc_c04,
    safe_cast(tmp_dc_c05 as int64) tmp_dc_c05,
    safe_cast(tmp_dc_c06 as int64) tmp_dc_c06,
    safe_cast(tmp_dc_c07 as int64) tmp_dc_c07,
    safe_cast(tmp_dc_c08 as int64) tmp_dc_c08,
    safe_cast(tmp_dc_c09 as int64) tmp_dc_c09,
    safe_cast(tmp_dc_c10 as int64) tmp_dc_c10,
    safe_cast(tmp_dc_c11 as int64) tmp_dc_c11,
    safe_cast(tmp_dc_c12 as int64) tmp_dc_c12,
    safe_cast(pre_mm_cyr as int64) pre_mm_cyr,
    safe_cast(pre_mm_uyr as int64) pre_mm_uyr,
    safe_cast(pre_mm_c01 as int64) pre_mm_c01,
    safe_cast(pre_mm_c02 as int64) pre_mm_c02,
    safe_cast(pre_mm_c03 as int64) pre_mm_c03,
    safe_cast(pre_mm_c04 as int64) pre_mm_c04,
    safe_cast(pre_mm_c05 as int64) pre_mm_c05,
    safe_cast(pre_mm_c06 as int64) pre_mm_c06,
    safe_cast(pre_mm_c07 as int64) pre_mm_c07,
    safe_cast(pre_mm_c08 as int64) pre_mm_c08,
    safe_cast(pre_mm_c09 as int64) pre_mm_c09,
    safe_cast(pre_mm_c10 as int64) pre_mm_c10,
    safe_cast(pre_mm_c11 as int64) pre_mm_c11,
    safe_cast(pre_mm_c12 as int64) pre_mm_c12,
    safe_cast(pet_mm_cyr as int64) pet_mm_cyr,
    safe_cast(pet_mm_uyr as int64) pet_mm_uyr,
    safe_cast(pet_mm_c01 as int64) pet_mm_c01,
    safe_cast(pet_mm_c02 as int64) pet_mm_c02,
    safe_cast(pet_mm_c03 as int64) pet_mm_c03,
    safe_cast(pet_mm_c04 as int64) pet_mm_c04,
    safe_cast(pet_mm_c05 as int64) pet_mm_c05,
    safe_cast(pet_mm_c06 as int64) pet_mm_c06,
    safe_cast(pet_mm_c07 as int64) pet_mm_c07,
    safe_cast(pet_mm_c08 as int64) pet_mm_c08,
    safe_cast(pet_mm_c09 as int64) pet_mm_c09,
    safe_cast(pet_mm_c10 as int64) pet_mm_c10,
    safe_cast(pet_mm_c11 as int64) pet_mm_c11,
    safe_cast(pet_mm_c12 as int64) pet_mm_c12,
    safe_cast(aet_mm_cyr as int64) aet_mm_cyr,
    safe_cast(aet_mm_uyr as int64) aet_mm_uyr,
    safe_cast(aet_mm_c01 as int64) aet_mm_c01,
    safe_cast(aet_mm_c02 as int64) aet_mm_c02,
    safe_cast(aet_mm_c03 as int64) aet_mm_c03,
    safe_cast(aet_mm_c04 as int64) aet_mm_c04,
    safe_cast(aet_mm_c05 as int64) aet_mm_c05,
    safe_cast(aet_mm_c06 as int64) aet_mm_c06,
    safe_cast(aet_mm_c07 as int64) aet_mm_c07,
    safe_cast(aet_mm_c08 as int64) aet_mm_c08,
    safe_cast(aet_mm_c09 as int64) aet_mm_c09,
    safe_cast(aet_mm_c10 as int64) aet_mm_c10,
    safe_cast(aet_mm_c11 as int64) aet_mm_c11,
    safe_cast(aet_mm_c12 as int64) aet_mm_c12,
    safe_cast(ari_ix_cav as int64) ari_ix_cav,
    safe_cast(ari_ix_uav as int64) ari_ix_uav,
    safe_cast(cmi_ix_cyr as int64) cmi_ix_cyr,
    safe_cast(cmi_ix_uyr as int64) cmi_ix_uyr,
    safe_cast(cmi_ix_c01 as int64) cmi_ix_c01,
    safe_cast(cmi_ix_c02 as int64) cmi_ix_c02,
    safe_cast(cmi_ix_c03 as int64) cmi_ix_c03,
    safe_cast(cmi_ix_c04 as int64) cmi_ix_c04,
    safe_cast(cmi_ix_c05 as int64) cmi_ix_c05,
    safe_cast(cmi_ix_c06 as int64) cmi_ix_c06,
    safe_cast(cmi_ix_c07 as int64) cmi_ix_c07,
    safe_cast(cmi_ix_c08 as int64) cmi_ix_c08,
    safe_cast(cmi_ix_c09 as int64) cmi_ix_c09,
    safe_cast(cmi_ix_c10 as int64) cmi_ix_c10,
    safe_cast(cmi_ix_c11 as int64) cmi_ix_c11,
    safe_cast(cmi_ix_c12 as int64) cmi_ix_c12,
    safe_cast(snw_pc_cyr as int64) snw_pc_cyr,
    safe_cast(snw_pc_uyr as int64) snw_pc_uyr,
    safe_cast(snw_pc_cmx as int64) snw_pc_cmx,
    safe_cast(snw_pc_c01 as int64) snw_pc_c01,
    safe_cast(snw_pc_c02 as int64) snw_pc_c02,
    safe_cast(snw_pc_c03 as int64) snw_pc_c03,
    safe_cast(snw_pc_c04 as int64) snw_pc_c04,
    safe_cast(snw_pc_c05 as int64) snw_pc_c05,
    safe_cast(snw_pc_c06 as int64) snw_pc_c06,
    safe_cast(snw_pc_c07 as int64) snw_pc_c07,
    safe_cast(snw_pc_c08 as int64) snw_pc_c08,
    safe_cast(snw_pc_c09 as int64) snw_pc_c09,
    safe_cast(snw_pc_c10 as int64) snw_pc_c10,
    safe_cast(snw_pc_c11 as int64) snw_pc_c11,
    safe_cast(snw_pc_c12 as int64) snw_pc_c12,
    safe_cast(glc_cl_cmj as int64) glc_cl_cmj,
    safe_cast(glc_pc_c01 as int64) glc_pc_c01,
    safe_cast(glc_pc_c02 as int64) glc_pc_c02,
    safe_cast(glc_pc_c03 as int64) glc_pc_c03,
    safe_cast(glc_pc_c04 as int64) glc_pc_c04,
    safe_cast(glc_pc_c05 as int64) glc_pc_c05,
    safe_cast(glc_pc_c06 as int64) glc_pc_c06,
    safe_cast(glc_pc_c07 as int64) glc_pc_c07,
    safe_cast(glc_pc_c08 as int64) glc_pc_c08,
    safe_cast(glc_pc_c09 as int64) glc_pc_c09,
    safe_cast(glc_pc_c10 as int64) glc_pc_c10,
    safe_cast(glc_pc_c11 as int64) glc_pc_c11,
    safe_cast(glc_pc_c12 as int64) glc_pc_c12,
    safe_cast(glc_pc_c13 as int64) glc_pc_c13,
    safe_cast(glc_pc_c14 as int64) glc_pc_c14,
    safe_cast(glc_pc_c15 as int64) glc_pc_c15,
    safe_cast(glc_pc_c16 as int64) glc_pc_c16,
    safe_cast(glc_pc_c17 as int64) glc_pc_c17,
    safe_cast(glc_pc_c18 as int64) glc_pc_c18,
    safe_cast(glc_pc_c19 as int64) glc_pc_c19,
    safe_cast(glc_pc_c20 as int64) glc_pc_c20,
    safe_cast(glc_pc_c21 as int64) glc_pc_c21,
    safe_cast(glc_pc_c22 as int64) glc_pc_c22,
    safe_cast(glc_pc_u01 as int64) glc_pc_u01,
    safe_cast(glc_pc_u02 as int64) glc_pc_u02,
    safe_cast(glc_pc_u03 as int64) glc_pc_u03,
    safe_cast(glc_pc_u04 as int64) glc_pc_u04,
    safe_cast(glc_pc_u05 as int64) glc_pc_u05,
    safe_cast(glc_pc_u06 as int64) glc_pc_u06,
    safe_cast(glc_pc_u07 as int64) glc_pc_u07,
    safe_cast(glc_pc_u08 as int64) glc_pc_u08,
    safe_cast(glc_pc_u09 as int64) glc_pc_u09,
    safe_cast(glc_pc_u10 as int64) glc_pc_u10,
    safe_cast(glc_pc_u11 as int64) glc_pc_u11,
    safe_cast(glc_pc_u12 as int64) glc_pc_u12,
    safe_cast(glc_pc_u13 as int64) glc_pc_u13,
    safe_cast(glc_pc_u14 as int64) glc_pc_u14,
    safe_cast(glc_pc_u15 as int64) glc_pc_u15,
    safe_cast(glc_pc_u16 as int64) glc_pc_u16,
    safe_cast(glc_pc_u17 as int64) glc_pc_u17,
    safe_cast(glc_pc_u18 as int64) glc_pc_u18,
    safe_cast(glc_pc_u19 as int64) glc_pc_u19,
    safe_cast(glc_pc_u20 as int64) glc_pc_u20,
    safe_cast(glc_pc_u21 as int64) glc_pc_u21,
    safe_cast(glc_pc_u22 as int64) glc_pc_u22,
    safe_cast(pnv_cl_cmj as int64) pnv_cl_cmj,
    safe_cast(pnv_pc_c01 as int64) pnv_pc_c01,
    safe_cast(pnv_pc_c02 as int64) pnv_pc_c02,
    safe_cast(pnv_pc_c03 as int64) pnv_pc_c03,
    safe_cast(pnv_pc_c04 as int64) pnv_pc_c04,
    safe_cast(pnv_pc_c05 as int64) pnv_pc_c05,
    safe_cast(pnv_pc_c06 as int64) pnv_pc_c06,
    safe_cast(pnv_pc_c07 as int64) pnv_pc_c07,
    safe_cast(pnv_pc_c08 as int64) pnv_pc_c08,
    safe_cast(pnv_pc_c09 as int64) pnv_pc_c09,
    safe_cast(pnv_pc_c10 as int64) pnv_pc_c10,
    safe_cast(pnv_pc_c11 as int64) pnv_pc_c11,
    safe_cast(pnv_pc_c12 as int64) pnv_pc_c12,
    safe_cast(pnv_pc_c13 as int64) pnv_pc_c13,
    safe_cast(pnv_pc_c14 as int64) pnv_pc_c14,
    safe_cast(pnv_pc_c15 as int64) pnv_pc_c15,
    safe_cast(pnv_pc_u01 as int64) pnv_pc_u01,
    safe_cast(pnv_pc_u02 as int64) pnv_pc_u02,
    safe_cast(pnv_pc_u03 as int64) pnv_pc_u03,
    safe_cast(pnv_pc_u04 as int64) pnv_pc_u04,
    safe_cast(pnv_pc_u05 as int64) pnv_pc_u05,
    safe_cast(pnv_pc_u06 as int64) pnv_pc_u06,
    safe_cast(pnv_pc_u07 as int64) pnv_pc_u07,
    safe_cast(pnv_pc_u08 as int64) pnv_pc_u08,
    safe_cast(pnv_pc_u09 as int64) pnv_pc_u09,
    safe_cast(pnv_pc_u10 as int64) pnv_pc_u10,
    safe_cast(pnv_pc_u11 as int64) pnv_pc_u11,
    safe_cast(pnv_pc_u12 as int64) pnv_pc_u12,
    safe_cast(pnv_pc_u13 as int64) pnv_pc_u13,
    safe_cast(pnv_pc_u14 as int64) pnv_pc_u14,
    safe_cast(pnv_pc_u15 as int64) pnv_pc_u15,
    safe_cast(wet_cl_cmj as int64) wet_cl_cmj,
    safe_cast(wet_pc_cg1 as int64) wet_pc_cg1,
    safe_cast(wet_pc_ug1 as int64) wet_pc_ug1,
    safe_cast(wet_pc_cg2 as int64) wet_pc_cg2,
    safe_cast(wet_pc_ug2 as int64) wet_pc_ug2,
    safe_cast(wet_pc_c01 as int64) wet_pc_c01,
    safe_cast(wet_pc_c02 as int64) wet_pc_c02,
    safe_cast(wet_pc_c03 as int64) wet_pc_c03,
    safe_cast(wet_pc_c04 as int64) wet_pc_c04,
    safe_cast(wet_pc_c05 as int64) wet_pc_c05,
    safe_cast(wet_pc_c06 as int64) wet_pc_c06,
    safe_cast(wet_pc_c07 as int64) wet_pc_c07,
    safe_cast(wet_pc_c08 as int64) wet_pc_c08,
    safe_cast(wet_pc_c09 as int64) wet_pc_c09,
    safe_cast(wet_pc_u01 as int64) wet_pc_u01,
    safe_cast(wet_pc_u02 as int64) wet_pc_u02,
    safe_cast(wet_pc_u03 as int64) wet_pc_u03,
    safe_cast(wet_pc_u04 as int64) wet_pc_u04,
    safe_cast(wet_pc_u05 as int64) wet_pc_u05,
    safe_cast(wet_pc_u06 as int64) wet_pc_u06,
    safe_cast(wet_pc_u07 as int64) wet_pc_u07,
    safe_cast(wet_pc_u08 as int64) wet_pc_u08,
    safe_cast(wet_pc_u09 as int64) wet_pc_u09,
    safe_cast(for_pc_cse as int64) for_pc_cse,
    safe_cast(for_pc_use as int64) for_pc_use,
    safe_cast(crp_pc_cse as int64) crp_pc_cse,
    safe_cast(crp_pc_use as int64) crp_pc_use,
    safe_cast(pst_pc_cse as int64) pst_pc_cse,
    safe_cast(pst_pc_use as int64) pst_pc_use,
    safe_cast(ire_pc_cse as int64) ire_pc_cse,
    safe_cast(ire_pc_use as int64) ire_pc_use,
    safe_cast(gla_pc_cse as int64) gla_pc_cse,
    safe_cast(gla_pc_use as int64) gla_pc_use,
    safe_cast(prm_pc_cse as int64) prm_pc_cse,
    safe_cast(prm_pc_use as int64) prm_pc_use,
    safe_cast(pac_pc_cse as int64) pac_pc_cse,
    safe_cast(pac_pc_use as int64) pac_pc_use,
    safe_cast(tbi_cl_cmj as int64) tbi_cl_cmj,
    safe_cast(tec_cl_cmj as int64) tec_cl_cmj,
    safe_cast(fmh_cl_cmj as int64) fmh_cl_cmj,
    safe_cast(fec_cl_cmj as int64) fec_cl_cmj,
    safe_cast(cly_pc_cav as int64) cly_pc_cav,
    safe_cast(cly_pc_uav as int64) cly_pc_uav,
    safe_cast(slt_pc_cav as int64) slt_pc_cav,
    safe_cast(slt_pc_uav as int64) slt_pc_uav,
    safe_cast(snd_pc_cav as int64) snd_pc_cav,
    safe_cast(snd_pc_uav as int64) snd_pc_uav,
    safe_cast(soc_th_cav as int64) soc_th_cav,
    safe_cast(soc_th_uav as int64) soc_th_uav,
    safe_cast(swc_pc_cyr as int64) swc_pc_cyr,
    safe_cast(swc_pc_uyr as int64) swc_pc_uyr,
    safe_cast(swc_pc_c01 as int64) swc_pc_c01,
    safe_cast(swc_pc_c02 as int64) swc_pc_c02,
    safe_cast(swc_pc_c03 as int64) swc_pc_c03,
    safe_cast(swc_pc_c04 as int64) swc_pc_c04,
    safe_cast(swc_pc_c05 as int64) swc_pc_c05,
    safe_cast(swc_pc_c06 as int64) swc_pc_c06,
    safe_cast(swc_pc_c07 as int64) swc_pc_c07,
    safe_cast(swc_pc_c08 as int64) swc_pc_c08,
    safe_cast(swc_pc_c09 as int64) swc_pc_c09,
    safe_cast(swc_pc_c10 as int64) swc_pc_c10,
    safe_cast(swc_pc_c11 as int64) swc_pc_c11,
    safe_cast(swc_pc_c12 as int64) swc_pc_c12,
    safe_cast(lit_cl_cmj as int64) lit_cl_cmj,
    safe_cast(kar_pc_cse as int64) kar_pc_cse,
    safe_cast(kar_pc_use as int64) kar_pc_use,
    safe_cast(ero_kh_cav as int64) ero_kh_cav,
    safe_cast(ero_kh_uav as int64) ero_kh_uav,
    safe_cast(pop_ct_csu as float64) pop_ct_csu,
    safe_cast(pop_ct_usu as float64) pop_ct_usu,
    safe_cast(ppd_pk_cav as float64) ppd_pk_cav,
    safe_cast(ppd_pk_uav as float64) ppd_pk_uav,
    safe_cast(urb_pc_cse as int64) urb_pc_cse,
    safe_cast(urb_pc_use as int64) urb_pc_use,
    safe_cast(nli_ix_cav as int64) nli_ix_cav,
    safe_cast(nli_ix_uav as int64) nli_ix_uav,
    safe_cast(rdd_mk_cav as int64) rdd_mk_cav,
    safe_cast(rdd_mk_uav as int64) rdd_mk_uav,
    safe_cast(hft_ix_c93 as int64) hft_ix_c93,
    safe_cast(hft_ix_u93 as int64) hft_ix_u93,
    safe_cast(hft_ix_c09 as int64) hft_ix_c09,
    safe_cast(hft_ix_u09 as int64) hft_ix_u09,
    safe_cast(gad_id_cmj as int64) gad_id_cmj,
    safe_cast(gdp_ud_cav as int64) gdp_ud_cav,
    safe_cast(gdp_ud_csu as int64) gdp_ud_csu,
    safe_cast(gdp_ud_usu as int64) gdp_ud_usu,
    safe_cast(hdi_ix_cav as int64) hdi_ix_cav,
    st_geogfromtext(geometry, make_valid => true) geometry
from {{ set_datalake_project("world_wwf_hydrosheds_staging.rivers_atlas") }} as t

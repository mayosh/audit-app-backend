webpackJsonp([3],{XoKs:function(e,t){},sPvw:function(e,t,s){"use strict";Object.defineProperty(t,"__esModule",{value:!0});var c=s("mtWM"),n=s.n(c),a={props:["customerId"],data:function(){return{displayResponse:null,accountId:this.customerId,checkNames:[],checks:{conversions_check:null,broad_modifiers_check:null,mobile_firendly_pages:null,low_quality_keywords:null,has_negatives:null,has_changes:null,has_more3_ads:null,search_ctr:null,ave_position:null,have_trials:null},sheetUrl:null}},methods:{getCheckFlag:function(e){var t=this,s="/api/check_account/"+this.customerId+"/"+e;n.a.get(s).then(function(s){console.log(s.data),t.checks[e]=s.data}).catch(function(e){console.log(e)})},getSheetUrl:function(){var e=this,t="/api/create_sheet/"+this.customerId;n.a.get(t).then(function(t){console.log(t.data),e.sheetUrl=t.data.url}).catch(function(e){console.log(e)})},flagText:function(e){switch(e){case"red":return"Test Failed";case"green":return"Test Passed";case"amber":return"Test Passed Partly";default:return"No data"}}},created:function(){for(var e in this.checks)this.checkNames.push(e),this.getCheckFlag(e);this.getSheetUrl()}},l={render:function(){var e=this,t=e.$createElement,s=e._self._c||t;return s("div",[s("p",[e._v("checklist page for customer "+e._s(e.accountId))]),e._v(" "),s("div",[e._l(e.checkNames,function(t){return s("div",{staticClass:"checllist_item"},[e.checks[t]?s("span",[e._v(" "+e._s(e.checks[t].description)+" "),s("span",{class:e.checks[t].flag},[e._v(" "+e._s(e.flagText(e.checks[t].flag)))])]):s("span",[e._v("Loaing check..")])])}),e._v(" "),s("div",{staticClass:"sheet_link"},[e.sheetUrl?s("span",[s("a",{staticClass:"sheet_button",attrs:{href:e.sheetUrl}},[e._v("Click to get sheet results")])]):s("span",[e._v("Loading sheet link..")])])],2)])},staticRenderFns:[]};var r=s("VU/8")(a,l,!1,function(e){s("XoKs")},null,null);t.default=r.exports}});
//# sourceMappingURL=3.5241694b84542385fdf6.js.map
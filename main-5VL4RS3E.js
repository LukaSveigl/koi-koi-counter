var wC=Object.defineProperty,CC=Object.defineProperties;var DC=Object.getOwnPropertyDescriptors;var Ap=Object.getOwnPropertySymbols;var xC=Object.prototype.hasOwnProperty,EC=Object.prototype.propertyIsEnumerable;var Np=(e,n,t)=>n in e?wC(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,_=(e,n)=>{for(var t in n||={})xC.call(n,t)&&Np(e,t,n[t]);if(Ap)for(var t of Ap(n))EC.call(n,t)&&Np(e,t,n[t]);return e},R=(e,n)=>CC(e,DC(n));var qe=null,fa=!1,Hd=1,IC=null,Ve=Symbol("SIGNAL");function S(e){let n=qe;return qe=e,n}function ga(){return qe}var Mi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ki(e){if(fa)throw new Error("");if(qe===null)return;qe.consumerOnSignalRead(e);let n=qe.producersTail;if(n!==void 0&&n.producer===e)return;let t,i=qe.recomputing;if(i&&(t=n!==void 0?n.nextProducer:qe.producers,t!==void 0&&t.producer===e)){qe.producersTail=t,t.lastReadVersion=e.version;return}let r=e.consumersTail;if(r!==void 0&&r.consumer===qe&&(!i||MC(r,qe)))return;let o=vr(qe),s={producer:e,consumer:qe,nextProducer:t,prevConsumer:r,lastReadVersion:e.version,nextConsumer:void 0};qe.producersTail=s,n!==void 0?n.nextProducer=s:qe.producers=s,o&&Lp(e,s)}function Op(){Hd++}function va(e){if(!(vr(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Hd)){if(!e.producerMustRecompute(e)&&!gr(e)){pa(e);return}e.producerRecomputeValue(e),pa(e)}}function Ud(e){if(e.consumers===void 0)return;let n=fa;fa=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||SC(i)}}finally{fa=n}}function zd(){return qe?.consumerAllowSignalWrites!==!1}function SC(e){e.dirty=!0,Ud(e),e.consumerMarkedDirty?.(e)}function pa(e){e.dirty=!1,e.lastCleanEpoch=Hd}function Qn(e){return e&&Fp(e),S(e)}function Fp(e){e.producersTail=void 0,e.recomputing=!0}function Ti(e,n){S(n),e&&Pp(e)}function Pp(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(vr(e))do t=$d(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function gr(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,i=n.lastReadVersion;if(i!==t.version||(va(t),i!==t.version))return!0}return!1}function Kn(e){if(vr(e)){let n=e.producers;for(;n!==void 0;)n=$d(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Lp(e,n){let t=e.consumersTail,i=vr(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!i)for(let r=e.producers;r!==void 0;r=r.nextProducer)Lp(r.producer,r)}function $d(e){let n=e.producer,t=e.nextProducer,i=e.nextConsumer,r=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!vr(n)){let o=n.producers;for(;o!==void 0;)o=$d(o)}return t}function vr(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function _a(e){IC?.(e)}function MC(e,n){let t=n.producersTail;if(t!==void 0){let i=n.producers;do{if(i===e)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function ya(e,n){return Object.is(e,n)}function Io(e,n){let t=Object.create(kC);t.computation=e,n!==void 0&&(t.equal=n);let i=()=>{if(va(t),ki(t),t.value===Eo)throw t.error;return t.value};return i[Ve]=t,_a(t),i}var ma=Symbol("UNSET"),ha=Symbol("COMPUTING"),Eo=Symbol("ERRORED"),kC=R(_({},Mi),{value:ma,dirty:!0,error:null,equal:ya,kind:"computed",producerMustRecompute(e){return e.value===ma||e.value===ha},producerRecomputeValue(e){if(e.value===ha)throw new Error("");let n=e.value;e.value=ha;let t=Qn(e),i,r=!1;try{i=e.computation(),S(null),r=n!==ma&&n!==Eo&&i!==Eo&&e.equal(n,i)}catch(o){i=Eo,e.error=o}finally{Ti(e,t)}if(r){e.value=n;return}e.value=i,e.version++}});function TC(){throw new Error}var Vp=TC;function jp(e){Vp(e)}function Gd(e){Vp=e}var RC=null;function Wd(e,n){let t=Object.create(So);t.value=e,n!==void 0&&(t.equal=n);let i=()=>Bp(t);return i[Ve]=t,_a(t),[i,s=>_r(t,s),s=>qd(t,s)]}function Bp(e){return ki(e),e.value}function _r(e,n){zd()||jp(e),e.equal(e.value,n)||(e.value=n,AC(e))}function qd(e,n){zd()||jp(e),_r(e,n(e.value))}var So=R(_({},Mi),{equal:ya,value:void 0,kind:"signal"});function AC(e){e.version++,Op(),Ud(e),RC?.(e)}var Yd=R(_({},Mi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Qd(e){if(e.dirty=!1,e.version>0&&!gr(e))return;e.version++;let n=Qn(e);try{e.cleanup(),e.fn()}finally{Ti(e,n)}}function U(e){return typeof e=="function"}function yr(e){let t=e(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var ba=yr(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ri(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var ae=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(U(i))try{i()}catch(o){n=o instanceof ba?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Hp(o)}catch(s){n=n??[],s instanceof ba?n=[...n,...s.errors]:n.push(s)}}if(n)throw new ba(n)}}add(n){var t;if(n&&n!==this)if(this.closed)Hp(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&Ri(t,n)}remove(n){let{_finalizers:t}=this;t&&Ri(t,n),n instanceof e&&n._removeParent(this)}};ae.EMPTY=(()=>{let e=new ae;return e.closed=!0,e})();var Kd=ae.EMPTY;function wa(e){return e instanceof ae||e&&"closed"in e&&U(e.remove)&&U(e.add)&&U(e.unsubscribe)}function Hp(e){U(e)?e():e.unsubscribe()}var Bt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var br={setTimeout(e,n,...t){let{delegate:i}=br;return i?.setTimeout?i.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=br;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Ca(e){br.setTimeout(()=>{let{onUnhandledError:n}=Bt;if(n)n(e);else throw e})}function Mo(){}var Up=Zd("C",void 0,void 0);function zp(e){return Zd("E",void 0,e)}function $p(e){return Zd("N",e,void 0)}function Zd(e,n,t){return{kind:e,value:n,error:t}}var Ai=null;function wr(e){if(Bt.useDeprecatedSynchronousErrorHandling){let n=!Ai;if(n&&(Ai={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:i}=Ai;if(Ai=null,t)throw i}}else e()}function Gp(e){Bt.useDeprecatedSynchronousErrorHandling&&Ai&&(Ai.errorThrown=!0,Ai.error=e)}var Ni=class extends ae{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,wa(n)&&n.add(this)):this.destination=FC}static create(n,t,i){return new xn(n,t,i)}next(n){this.isStopped?Jd($p(n),this):this._next(n)}error(n){this.isStopped?Jd(zp(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Jd(Up,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},NC=Function.prototype.bind;function Xd(e,n){return NC.call(e,n)}var eu=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(i){Da(i)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(i){Da(i)}else Da(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){Da(t)}}},xn=class extends Ni{constructor(n,t,i){super();let r;if(U(n)||!n)r={next:n??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&Bt.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Xd(n.next,o),error:n.error&&Xd(n.error,o),complete:n.complete&&Xd(n.complete,o)}):r=n}this.destination=new eu(r)}};function Da(e){Bt.useDeprecatedSynchronousErrorHandling?Gp(e):Ca(e)}function OC(e){throw e}function Jd(e,n){let{onStoppedNotification:t}=Bt;t&&br.setTimeout(()=>t(e,n))}var FC={closed:!0,next:Mo,error:OC,complete:Mo};var Cr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function ft(e){return e}function tu(...e){return nu(e)}function nu(e){return e.length===0?ft:e.length===1?e[0]:function(t){return e.reduce((i,r)=>r(i),t)}}var ee=(()=>{class e{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new e;return i.source=this,i.operator=t,i}subscribe(t,i,r){let o=LC(t)?t:new xn(t,i,r);return wr(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Wp(i),new i((r,o)=>{let s=new xn({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Cr](){return this}pipe(...t){return nu(t)(this)}toPromise(t){return t=Wp(t),new t((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return e.create=n=>new e(n),e})();function Wp(e){var n;return(n=e??Bt.Promise)!==null&&n!==void 0?n:Promise}function PC(e){return e&&U(e.next)&&U(e.error)&&U(e.complete)}function LC(e){return e&&e instanceof Ni||PC(e)&&wa(e)}function VC(e){return U(e?.lift)}function q(e){return n=>{if(VC(n))return n.lift(function(t){try{return e(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Q(e,n,t,i,r){return new iu(e,n,t,i,r)}var iu=class extends Ni{constructor(n,t,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var qp=yr(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=(()=>{class e extends ee{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new xa(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new qp}next(t){wr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){wr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){wr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Kd:(this.currentObservers=null,o.push(t),new ae(()=>{this.currentObservers=null,Ri(o,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:o}=this;i?t.error(r):o&&t.complete()}asObservable(){let t=new ee;return t.source=this,t}}return e.create=(n,t)=>new xa(n,t),e})(),xa=class extends E{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,n)}error(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&i!==void 0?i:Kd}};var je=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:i}=this;if(n)throw t;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var ko={now(){return(ko.delegate||Date).now()},delegate:void 0};var Ea=class extends E{constructor(n=1/0,t=1/0,i=ko){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;t||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=t.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Ia=class extends ae{constructor(n,t){super()}schedule(n,t=0){return this}};var To={setInterval(e,n,...t){let{delegate:i}=To;return i?.setInterval?i.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=To;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var Sa=class extends Ia{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,t)),this.pending=!0,this.delay=t,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,i=0){return To.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,t,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return t;t!=null&&To.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,t);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:i}=t;this.work=this.state=this.scheduler=null,this.pending=!1,Ri(i,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var Dr=class e{constructor(n,t=e.now){this.schedulerActionCtor=n,this.now=t}schedule(n,t=0,i){return new this.schedulerActionCtor(this,n).schedule(i,t)}};Dr.now=ko.now;var Ma=class extends Dr{constructor(n,t=Dr.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,i){for(;n=t.shift();)n.unsubscribe();throw i}}};var Ro=new Ma(Sa),Yp=Ro;var Se=new ee(e=>e.complete());function ka(e){return e&&U(e.schedule)}function ru(e){return e[e.length-1]}function Ta(e){return U(ru(e))?e.pop():void 0}function tn(e){return ka(ru(e))?e.pop():void 0}function Qp(e,n){return typeof ru(e)=="number"?e.pop():n}function Zp(e,n,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(e,n||[])).next())})}function Kp(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],i=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&i>=e.length&&(e=void 0),{value:e&&e[i++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Oi(e){return this instanceof Oi?(this.v=e,this):new Oi(e)}function Xp(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(e,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(h){return function(g){return Promise.resolve(g).then(h,f)}}function a(h,g){i[h]&&(r[h]=function(C){return new Promise(function(W,ne){o.push([h,C,W,ne])>1||c(h,C)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(C){m(o[0][3],C)}}function l(h){h.value instanceof Oi?Promise.resolve(h.value.v).then(d,f):m(o[0][2],h)}function d(h){c("next",h)}function f(h){c("throw",h)}function m(h,g){h(g),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Jp(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof Kp=="function"?Kp(e):e[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Ra=e=>e&&typeof e.length=="number"&&typeof e!="function";function Aa(e){return U(e?.then)}function Na(e){return U(e[Cr])}function Oa(e){return Symbol.asyncIterator&&U(e?.[Symbol.asyncIterator])}function Fa(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function jC(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Pa=jC();function La(e){return U(e?.[Pa])}function Va(e){return Xp(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:i,done:r}=yield Oi(t.read());if(r)return yield Oi(void 0);yield yield Oi(i)}}finally{t.releaseLock()}})}function ja(e){return U(e?.getReader)}function ve(e){if(e instanceof ee)return e;if(e!=null){if(Na(e))return BC(e);if(Ra(e))return HC(e);if(Aa(e))return UC(e);if(Oa(e))return eg(e);if(La(e))return zC(e);if(ja(e))return $C(e)}throw Fa(e)}function BC(e){return new ee(n=>{let t=e[Cr]();if(U(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function HC(e){return new ee(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function UC(e){return new ee(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,Ca)})}function zC(e){return new ee(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function eg(e){return new ee(n=>{GC(e,n).catch(t=>n.error(t))})}function $C(e){return eg(Va(e))}function GC(e,n){var t,i,r,o;return Zp(this,void 0,void 0,function*(){try{for(t=Jp(e);i=yield t.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}n.complete()})}function ct(e,n,t,i=0,r=!1){let o=n.schedule(function(){t(),r?e.add(this.schedule(null,i)):this.unsubscribe()},i);if(e.add(o),!r)return o}function Ba(e,n=0){return q((t,i)=>{t.subscribe(Q(i,r=>ct(i,e,()=>i.next(r),n),()=>ct(i,e,()=>i.complete(),n),r=>ct(i,e,()=>i.error(r),n)))})}function Ha(e,n=0){return q((t,i)=>{i.add(e.schedule(()=>t.subscribe(i),n))})}function tg(e,n){return ve(e).pipe(Ha(n),Ba(n))}function ng(e,n){return ve(e).pipe(Ha(n),Ba(n))}function ig(e,n){return new ee(t=>{let i=0;return n.schedule(function(){i===e.length?t.complete():(t.next(e[i++]),t.closed||this.schedule())})})}function rg(e,n){return new ee(t=>{let i;return ct(t,n,()=>{i=e[Pa](),ct(t,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>U(i?.return)&&i.return()})}function Ua(e,n){if(!e)throw new Error("Iterable cannot be null");return new ee(t=>{ct(t,n,()=>{let i=e[Symbol.asyncIterator]();ct(t,n,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function og(e,n){return Ua(Va(e),n)}function sg(e,n){if(e!=null){if(Na(e))return tg(e,n);if(Ra(e))return ig(e,n);if(Aa(e))return ng(e,n);if(Oa(e))return Ua(e,n);if(La(e))return rg(e,n);if(ja(e))return og(e,n)}throw Fa(e)}function be(e,n){return n?sg(e,n):ve(e)}function Z(...e){let n=tn(e);return be(e,n)}function ou(e,n){let t=U(e)?e:()=>e,i=r=>r.error(t());return new ee(n?r=>n.schedule(i,0,r):i)}function Ao(e){return!!e&&(e instanceof ee||U(e.lift)&&U(e.subscribe))}var Fi=yr(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function ag(e){return e instanceof Date&&!isNaN(e)}function ce(e,n){return q((t,i)=>{let r=0;t.subscribe(Q(i,o=>{i.next(e.call(n,o,r++))}))})}var{isArray:WC}=Array;function qC(e,n){return WC(n)?e(...n):e(n)}function za(e){return ce(n=>qC(e,n))}var{isArray:YC}=Array,{getPrototypeOf:QC,prototype:KC,keys:ZC}=Object;function $a(e){if(e.length===1){let n=e[0];if(YC(n))return{args:n,keys:null};if(XC(n)){let t=ZC(n);return{args:t.map(i=>n[i]),keys:t}}}return{args:e,keys:null}}function XC(e){return e&&typeof e=="object"&&QC(e)===KC}function Ga(e,n){return e.reduce((t,i,r)=>(t[i]=n[r],t),{})}function su(...e){let n=tn(e),t=Ta(e),{args:i,keys:r}=$a(e);if(i.length===0)return be([],n);let o=new ee(JC(i,n,r?s=>Ga(r,s):ft));return t?o.pipe(za(t)):o}function JC(e,n,t=ft){return i=>{cg(n,()=>{let{length:r}=e,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)cg(n,()=>{let l=be(e[c],n),d=!1;l.subscribe(Q(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(t(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function cg(e,n,t){e?ct(t,e,n):n()}function lg(e,n,t,i,r,o,s,a){let c=[],l=0,d=0,f=!1,m=()=>{f&&!c.length&&!l&&n.complete()},h=C=>l<i?g(C):c.push(C),g=C=>{o&&n.next(C),l++;let W=!1;ve(t(C,d++)).subscribe(Q(n,ne=>{r?.(ne),o?h(ne):n.next(ne)},()=>{W=!0},void 0,()=>{if(W)try{for(l--;c.length&&l<i;){let ne=c.shift();s?ct(n,s,()=>g(ne)):g(ne)}m()}catch(ne){n.error(ne)}}))};return e.subscribe(Q(n,h,()=>{f=!0,m()})),()=>{a?.()}}function rt(e,n,t=1/0){return U(n)?rt((i,r)=>ce((o,s)=>n(i,o,r,s))(ve(e(i,r))),t):(typeof n=="number"&&(t=n),q((i,r)=>lg(i,r,e,t)))}function Wa(e=1/0){return rt(ft,e)}function dg(){return Wa(1)}function xr(...e){return dg()(be(e,tn(e)))}function Pi(e){return new ee(n=>{ve(e()).subscribe(n)})}function au(...e){let n=Ta(e),{args:t,keys:i}=$a(e),r=new ee(o=>{let{length:s}=t;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;ve(t[d]).subscribe(Q(o,m=>{f||(f=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?Ga(i,a):a),o.complete())}))}});return n?r.pipe(za(n)):r}function ug(e=0,n,t=Yp){let i=-1;return n!=null&&(ka(n)?t=n:i=n),new ee(r=>{let o=ag(e)?+e-t.now():e;o<0&&(o=0);let s=0;return t.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Li(...e){let n=tn(e),t=Qp(e,1/0),i=e;return i.length?i.length===1?ve(i[0]):Wa(t)(be(i,n)):Se}function we(e,n){return q((t,i)=>{let r=0;t.subscribe(Q(i,o=>e.call(n,o,r++)&&i.next(o)))})}function fg(e){return q((n,t)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,t.next(l)}s&&t.complete()},c=()=>{o=null,s&&t.complete()};n.subscribe(Q(t,l=>{i=!0,r=l,o||ve(e(l)).subscribe(o=Q(t,a,c))},()=>{s=!0,(!i||!o||o.closed)&&t.complete()}))})}function qa(e,n=Ro){return fg(()=>ug(e,n))}function No(e){return q((n,t)=>{let i=null,r=!1,o;i=n.subscribe(Q(t,void 0,void 0,s=>{o=ve(e(s,No(e)(n))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function Ya(e,n){return U(n)?rt(e,n,1):rt(e,1)}function cu(e,n=Ro){return q((t,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+e,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}t.subscribe(Q(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,e),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function mg(e){return q((n,t)=>{let i=!1;n.subscribe(Q(t,r=>{i=!0,t.next(r)},()=>{i||t.next(e),t.complete()}))})}function Ht(e){return e<=0?()=>Se:q((n,t)=>{let i=0;n.subscribe(Q(t,r=>{++i<=e&&(t.next(r),e<=i&&t.complete())}))})}function Qa(e,n=ft){return e=e??eD,q((t,i)=>{let r,o=!0;t.subscribe(Q(i,s=>{let a=n(s);(o||!e(r,a))&&(o=!1,r=a,i.next(s))}))})}function eD(e,n){return e===n}function hg(e=tD){return q((n,t)=>{let i=!1;n.subscribe(Q(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(e())))})}function tD(){return new Fi}function lu(e){return q((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function En(e,n){let t=arguments.length>=2;return i=>i.pipe(e?we((r,o)=>e(r,o,i)):ft,Ht(1),t?mg(n):hg(()=>new Fi))}function Ka(e){return e<=0?()=>Se:q((n,t)=>{let i=[];n.subscribe(Q(t,r=>{i.push(r),e<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Za(){return q((e,n)=>{let t,i=!1;e.subscribe(Q(n,r=>{let o=t;t=r,i&&n.next([o,r]),i=!0}))})}function pg(e={}){let{connector:n=()=>new E,resetOnError:t=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=e;return o=>{let s,a,c,l=0,d=!1,f=!1,m=()=>{a?.unsubscribe(),a=void 0},h=()=>{m(),s=c=void 0,d=f=!1},g=()=>{let C=s;h(),C?.unsubscribe()};return q((C,W)=>{l++,!f&&!d&&m();let ne=c=c??n();W.add(()=>{l--,l===0&&!f&&!d&&(a=du(g,r))}),ne.subscribe(W),!s&&l>0&&(s=new xn({next:Re=>ne.next(Re),error:Re=>{f=!0,m(),a=du(h,t,Re),ne.error(Re)},complete:()=>{d=!0,m(),a=du(h,i),ne.complete()}}),ve(C).subscribe(s))})(o)}}function du(e,n,...t){if(n===!0){e();return}if(n===!1)return;let i=new xn({next:()=>{i.unsubscribe(),e()}});return ve(n(...t)).subscribe(i)}function Xa(e,n,t){let i,r=!1;return e&&typeof e=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:t}=e:i=e??1/0,pg({connector:()=>new Ea(i,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function uu(e){return we((n,t)=>e<=t)}function In(...e){let n=tn(e);return q((t,i)=>{(n?xr(e,t,n):xr(e,t)).subscribe(i)})}function ot(e,n){return q((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(Q(i,c=>{r?.unsubscribe();let l=0,d=o++;ve(e(c,d)).subscribe(r=Q(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function $e(e){return q((n,t)=>{ve(e).subscribe(Q(t,()=>t.complete(),Mo)),!t.closed&&n.subscribe(t)})}function fu(e,n=!1){return q((t,i)=>{let r=0;t.subscribe(Q(i,o=>{let s=e(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function lt(e,n,t){let i=U(e)||n||t?{next:e,error:n,complete:t}:e;return i?q((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(Q(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):ft}var mu;function Ja(){return mu}function nn(e){let n=mu;return mu=e,n}var gg=Symbol("NotFound");function Er(e){return e===gg||e?.name==="\u0275NotFound"}function vg(e){let n=S(null);try{return e()}finally{S(n)}}var sc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(n,t){super(Ut(n,t)),this.code=n}};function nD(e){return`NG0${Math.abs(e)}`}function Ut(e,n){return`${nD(e)}${n?": "+n:""}`}var Sr=globalThis;function ue(e){for(let n in e)if(e[n]===ue)return n;throw Error("")}function Cg(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function Bo(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(Bo).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function ac(e,n){return e?n?`${e} ${n}`:e:n||""}var iD=ue({__forward_ref__:ue});function ht(e){return e.__forward_ref__=ht,e}function Ge(e){return Iu(e)?e():e}function Iu(e){return typeof e=="function"&&e.hasOwnProperty(iD)&&e.__forward_ref__===ht}function w(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function F(e){return{providers:e.providers||[],imports:e.imports||[]}}function Ho(e){return rD(e,cc)}function Su(e){return Ho(e)!==null}function rD(e,n){return e.hasOwnProperty(n)&&e[n]||null}function oD(e){let n=e?.[cc]??null;return n||null}function pu(e){return e&&e.hasOwnProperty(tc)?e[tc]:null}var cc=ue({\u0275prov:ue}),tc=ue({\u0275inj:ue}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=w({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Mu(e){return e&&!!e.\u0275providers}var ku=ue({\u0275cmp:ue}),Tu=ue({\u0275dir:ue}),Ru=ue({\u0275pipe:ue}),Au=ue({\u0275mod:ue}),Fo=ue({\u0275fac:ue}),zi=ue({__NG_ELEMENT_ID__:ue}),_g=ue({__NG_ENV_ID__:ue});function Nu(e){return lc(e,"@NgModule"),e[Au]||null}function kn(e){return lc(e,"@Component"),e[ku]||null}function Ou(e){return lc(e,"@Directive"),e[Tu]||null}function Dg(e){return lc(e,"@Pipe"),e[Ru]||null}function lc(e,n){if(e==null)throw new x(-919,!1)}function Uo(e){return typeof e=="string"?e:e==null?"":String(e)}var xg=ue({ngErrorCode:ue}),sD=ue({ngErrorMessage:ue}),aD=ue({ngTokenPath:ue});function Fu(e,n){return Eg("",-200,n)}function dc(e,n){throw new x(-201,!1)}function Eg(e,n,t){let i=new x(n,e);return i[xg]=n,i[sD]=e,t&&(i[aD]=t),i}function cD(e){return e[xg]}var gu;function Ig(){return gu}function mt(e){let n=gu;return gu=e,n}function Pu(e,n,t){let i=Ho(e);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(n!==void 0)return n;dc(e,"")}var lD={},Vi=lD,dD="__NG_DI_FLAG__",vu=class{injector;constructor(n){this.injector=n}retrieve(n,t){let i=ji(t)||0;try{return this.injector.get(n,i&8?null:Vi,i)}catch(r){if(Er(r))return r;throw r}}};function uD(e,n=0){let t=Ja();if(t===void 0)throw new x(-203,!1);if(t===null)return Pu(e,void 0,n);{let i=fD(n),r=t.retrieve(e,i);if(Er(r)){if(i.optional)return null;throw r}return r}}function z(e,n=0){return(Ig()||uD)(Ge(e),n)}function u(e,n){return z(e,ji(n))}function ji(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function fD(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function _u(e){let n=[];for(let t=0;t<e.length;t++){let i=Ge(e[t]);if(Array.isArray(i)){if(i.length===0)throw new x(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=mD(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(z(r,o))}else n.push(z(i))}return n}function mD(e){return e[dD]}function Bi(e,n){let t=e.hasOwnProperty(Fo);return t?e[Fo]:null}function Sg(e,n,t){if(e.length!==n.length)return!1;for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function Mg(e){return e.flat(Number.POSITIVE_INFINITY)}function uc(e,n){e.forEach(t=>Array.isArray(t)?uc(t,n):n(t))}function Lu(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function zo(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function kg(e,n){let t=[];for(let i=0;i<e;i++)t.push(n);return t}function Tg(e,n,t,i){let r=e.length;if(r==n)e.push(t,i);else if(r===1)e.push(i,e[0]),e[0]=t;else{for(r--,e.push(e[r-1],e[r]);r>n;){let o=r-2;e[r]=e[o],r--}e[n]=t,e[n+1]=i}}function fc(e,n,t){let i=Mr(e,n);return i>=0?e[i|1]=t:(i=~i,Tg(e,i,n,t)),i}function mc(e,n){let t=Mr(e,n);if(t>=0)return e[t|1]}function Mr(e,n){return hD(e,n,1)}function hD(e,n,t){let i=0,r=e.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=e[o<<t];if(n===s)return o<<t;s>n?r=o:i=o+1}return~(r<<t)}var Jn={},Ye=[],ei=new y(""),Vu=new y("",-1),ju=new y(""),Po=class{get(n,t=Vi){if(t===Vi){let r=Eg("",-201);throw r.name="\u0275NotFound",r}return t}};function $i(e){return{\u0275providers:e}}function Rg(e){return $i([{provide:ei,multi:!0,useValue:e}])}function Ag(...e){return{\u0275providers:Bu(!0,e),\u0275fromNgModule:!0}}function Bu(e,...n){let t=[],i=new Set,r,o=s=>{t.push(s)};return uc(n,s=>{let a=s;nc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Ng(r,o),t}function Ng(e,n){for(let t=0;t<e.length;t++){let{ngModule:i,providers:r}=e[t];Hu(r,o=>{n(o,i)})}}function nc(e,n,t,i){if(e=Ge(e),!e)return!1;let r=null,o=pu(e),s=!o&&kn(e);if(!o&&!s){let c=e.ngModule;if(o=pu(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=e}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)nc(l,n,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;uc(o.imports,d=>{nc(d,n,t,i)&&(l||=[],l.push(d))}),l!==void 0&&Ng(l,n)}if(!a){let l=Bi(r)||(()=>new r);n({provide:r,useFactory:l,deps:Ye},r),n({provide:ju,useValue:r,multi:!0},r),n({provide:ei,useValue:()=>z(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=e;Hu(c,d=>{n(d,l)})}}else return!1;return r!==e&&e.providers!==void 0}function Hu(e,n){for(let t of e)Mu(t)&&(t=t.\u0275providers),Array.isArray(t)?Hu(t,n):n(t)}var pD=ue({provide:String,useValue:ue});function Og(e){return e!==null&&typeof e=="object"&&pD in e}function gD(e){return!!(e&&e.useExisting)}function vD(e){return!!(e&&e.useFactory)}function Hi(e){return typeof e=="function"}function Fg(e){return!!e.useClass}var $o=new y(""),ec={},yg={},hu;function kr(){return hu===void 0&&(hu=new Po),hu}var ye=class{},Ui=class extends ye{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,bu(n,s=>this.processProvider(s)),this.records.set(Vu,Ir(void 0,this)),r.has("environment")&&this.records.set(ye,Ir(void 0,this));let o=this.records.get($o);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(ju,Ye,{self:!0}))}retrieve(n,t){let i=ji(t)||0;try{return this.get(n,Vi,i)}catch(r){if(Er(r))return r;throw r}}destroy(){Oo(this),this._destroyed=!0;let n=S(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),S(n)}}onDestroy(n){return Oo(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Oo(this);let t=nn(this),i=mt(void 0),r;try{return n()}finally{nn(t),mt(i)}}get(n,t=Vi,i){if(Oo(this),n.hasOwnProperty(_g))return n[_g](this);let r=ji(i),o,s=nn(this),a=mt(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=CD(n)&&Ho(n);d&&this.injectableDefInScope(d)?l=Ir(yu(n),ec):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?kr():this.parent;return t=r&8&&t===Vi?null:t,c.get(n,t)}catch(c){let l=cD(c);throw l===-200||l===-201?new x(l,null):c}finally{mt(a),nn(s)}}resolveInjectorInitializers(){let n=S(null),t=nn(this),i=mt(void 0),r;try{let o=this.get(ei,Ye,{self:!0});for(let s of o)s()}finally{nn(t),mt(i),S(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Ge(n);let t=Hi(n)?n:Ge(n&&n.provide),i=yD(n);if(!Hi(n)&&n.multi===!0){let r=this.records.get(t);r||(r=Ir(void 0,ec,!0),r.factory=()=>_u(r.multi),this.records.set(t,r)),t=n,r.multi.push(n)}this.records.set(t,i)}hydrate(n,t,i){let r=S(null);try{if(t.value===yg)throw Fu("");return t.value===ec&&(t.value=yg,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&wD(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{S(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=Ge(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function yu(e){let n=Ho(e),t=n!==null?n.factory:Bi(e);if(t!==null)return t;if(e instanceof y)throw new x(-204,!1);if(e instanceof Function)return _D(e);throw new x(-204,!1)}function _D(e){if(e.length>0)throw new x(-204,!1);let t=oD(e);return t!==null?()=>t.factory(e):()=>new e}function yD(e){if(Og(e))return Ir(void 0,e.useValue);{let n=Uu(e);return Ir(n,ec)}}function Uu(e,n,t){let i;if(Hi(e)){let r=Ge(e);return Bi(r)||yu(r)}else if(Og(e))i=()=>Ge(e.useValue);else if(vD(e))i=()=>e.useFactory(..._u(e.deps||[]));else if(gD(e))i=(r,o)=>z(Ge(e.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Ge(e&&(e.useClass||e.provide));if(bD(e))i=()=>new r(..._u(e.deps));else return Bi(r)||yu(r)}return i}function Oo(e){if(e.destroyed)throw new x(-205,!1)}function Ir(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function bD(e){return!!e.deps}function wD(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function CD(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function bu(e,n){for(let t of e)Array.isArray(t)?bu(t,n):t&&Mu(t)?bu(t.\u0275providers,n):n(t)}function Qe(e,n){let t;e instanceof Ui?(Oo(e),t=e):t=new vu(e);let i,r=nn(t),o=mt(void 0);try{return n()}finally{nn(r),mt(o)}}function Pg(){return Ig()!==void 0||Ja()!=null}var zt=0,A=1,L=2,Be=3,Et=4,st=5,Gi=6,Tr=7,Me=8,Tn=9,rn=10,Ce=11,Rr=12,zu=13,Wi=14,at=15,ti=16,qi=17,on=18,Rn=19,$u=20,Sn=21,hc=22,Zn=23,pt=24,Yi=25,ni=26,ke=27,Lg=1,Gu=6,ii=7,Go=8,Qi=9,Ie=10;function An(e){return Array.isArray(e)&&typeof e[Lg]=="object"}function $t(e){return Array.isArray(e)&&e[Lg]===!0}function Wu(e){return(e.flags&4)!==0}function Nn(e){return e.componentOffset>-1}function Wo(e){return(e.flags&1)===1}function sn(e){return!!e.template}function Ar(e){return(e[L]&512)!==0}function Ki(e){return(e[L]&256)===256}var qu="svg",Vg="math";function It(e){for(;Array.isArray(e);)e=e[zt];return e}function Yu(e,n){return It(n[e])}function Gt(e,n){return It(n[e.index])}function pc(e,n){return e.data[n]}function jg(e,n){return e[n]}function St(e,n){let t=n[e];return An(t)?t:t[zt]}function Bg(e){return(e[L]&4)===4}function gc(e){return(e[L]&128)===128}function Hg(e){return $t(e[Be])}function Mt(e,n){return n==null?null:e[n]}function Qu(e){e[qi]=0}function Ku(e){e[L]&1024||(e[L]|=1024,gc(e)&&Zi(e))}function Ug(e,n){for(;e>0;)n=n[Wi],e--;return n}function qo(e){return!!(e[L]&9216||e[pt]?.dirty)}function vc(e){e[rn].changeDetectionScheduler?.notify(8),e[L]&64&&(e[L]|=1024),qo(e)&&Zi(e)}function Zi(e){e[rn].changeDetectionScheduler?.notify(0);let n=Xn(e);for(;n!==null&&!(n[L]&8192||(n[L]|=8192,!gc(n)));)n=Xn(n)}function Zu(e,n){if(Ki(e))throw new x(911,!1);e[Sn]===null&&(e[Sn]=[]),e[Sn].push(n)}function zg(e,n){if(e[Sn]===null)return;let t=e[Sn].indexOf(n);t!==-1&&e[Sn].splice(t,1)}function Xn(e){let n=e[Be];return $t(n)?n[Be]:n}function Xu(e){return e[Tr]??=[]}function Ju(e){return e.cleanup??=[]}function $g(e,n,t,i){let r=Xu(n);r.push(t),e.firstCreatePass&&Ju(e).push(i,r.length-1)}var X={lFrame:nv(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var wu=!1;function Gg(){return X.lFrame.elementDepthCount}function Wg(){X.lFrame.elementDepthCount++}function ef(){X.lFrame.elementDepthCount--}function tf(){return X.bindingsEnabled}function nf(){return X.skipHydrationRootTNode!==null}function rf(e){return X.skipHydrationRootTNode===e}function of(){X.skipHydrationRootTNode=null}function K(){return X.lFrame.lView}function Ae(){return X.lFrame.tView}function gt(e){return X.lFrame.contextLView=e,e[Me]}function vt(e){return X.lFrame.contextLView=null,e}function Ke(){let e=sf();for(;e!==null&&e.type===64;)e=e.parent;return e}function sf(){return X.lFrame.currentTNode}function qg(){let e=X.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function Nr(e,n){let t=X.lFrame;t.currentTNode=e,t.isParent=n}function af(){return X.lFrame.isParent}function cf(){X.lFrame.isParent=!1}function Yg(){return X.lFrame.contextLView}function lf(){return wu}function Lo(e){let n=wu;return wu=e,n}function Qg(){return X.lFrame.bindingIndex}function Kg(e){return X.lFrame.bindingIndex=e}function Xi(){return X.lFrame.bindingIndex++}function _c(e){let n=X.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function Zg(){return X.lFrame.inI18n}function Xg(e,n){let t=X.lFrame;t.bindingIndex=t.bindingRootIndex=e,yc(n)}function Jg(){return X.lFrame.currentDirectiveIndex}function yc(e){X.lFrame.currentDirectiveIndex=e}function ev(e){let n=X.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function bc(){return X.lFrame.currentQueryIndex}function Yo(e){X.lFrame.currentQueryIndex=e}function DD(e){let n=e[A];return n.type===2?n.declTNode:n.type===1?e[st]:null}function df(e,n,t){if(t&4){let r=n,o=e;for(;r=r.parent,r===null&&!(t&1);)if(r=DD(o),r===null||(o=o[Wi],r.type&10))break;if(r===null)return!1;n=r,e=o}let i=X.lFrame=tv();return i.currentTNode=n,i.lView=e,!0}function wc(e){let n=tv(),t=e[A];X.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function tv(){let e=X.lFrame,n=e===null?null:e.child;return n===null?nv(e):n}function nv(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function iv(){let e=X.lFrame;return X.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var uf=iv;function Cc(){let e=iv();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function rv(e){return(X.lFrame.contextLView=Ug(e,X.lFrame.contextLView))[Me]}function an(){return X.lFrame.selectedIndex}function ri(e){X.lFrame.selectedIndex=e}function Dc(){let e=X.lFrame;return pc(e.tView,e.selectedIndex)}function Ji(){X.lFrame.currentNamespace=qu}function ov(){return X.lFrame.currentNamespace}var sv=!0;function xc(){return sv}function Ec(e){sv=e}function Cu(e,n=null,t=null,i){let r=ff(e,n,t,i);return r.resolveInjectorInitializers(),r}function ff(e,n=null,t=null,i,r=new Set){let o=[t||Ye,Ag(e)],s;return new Ui(o,n||kr(),s||null,r)}var J=class e{static THROW_IF_NOT_FOUND=Vi;static NULL=new Po;static create(n,t){if(Array.isArray(n))return Cu({name:""},t,n,"");{let i=n.name??"";return Cu({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:e,providedIn:"any",factory:()=>z(Vu)});static __NG_ELEMENT_ID__=-1},Y=new y(""),Ze=(()=>{class e{static __NG_ELEMENT_ID__=xD;static __NG_ENV_ID__=t=>t}return e})(),ic=class extends Ze{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Ki(this._lView)}onDestroy(n){let t=this._lView;return Zu(t,n),()=>zg(t,n)}};function xD(){return new ic(K())}var av=!1,cv=new y(""),oi=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new je(!1);debugTaskTracker=u(cv,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ee(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:e,providedIn:"root",factory:()=>new e})}return e})(),Du=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Pg()&&(this.destroyRef=u(Ze,{optional:!0})??void 0,this.pendingTasks=u(oi,{optional:!0})??void 0)}emit(n){let t=S(null);try{super.next(n)}finally{S(t)}}subscribe(n,t,i){let r=n,o=t||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ae&&n.add(a),a}wrapInTimeout(n){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},j=Du;function rc(...e){}function mf(e){let n,t;function i(){e=rc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),i()})),()=>i()}function lv(e){return queueMicrotask(()=>e()),()=>{e=rc}}var hf="isAngularZone",Vo=hf+"_ID",ED=0,T=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new j(!1);onMicrotaskEmpty=new j(!1);onStable=new j(!1);onError=new j(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=av}=n;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,MD(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(hf)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new x(909,!1)}run(n,t,i){return this._inner.run(n,t,i)}runTask(n,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,ID,rc,rc);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(n,t,i){return this._inner.runGuarded(n,t,i)}runOutsideAngular(n){return this._outer.run(n)}},ID={};function pf(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function SD(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){mf(()=>{e.callbackScheduled=!1,xu(e),e.isCheckStableRunning=!0,pf(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),xu(e)}function MD(e){let n=()=>{SD(e)},t=ED++;e._inner=e._inner.fork({name:"angular",properties:{[hf]:!0,[Vo]:t,[Vo+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(kD(c))return i.invokeTask(o,s,a,c);try{return bg(e),i.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),wg(e)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return bg(e),i.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!TD(c)&&n(),wg(e)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,xu(e),pf(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function xu(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function bg(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function wg(e){e._nesting--,pf(e)}var jo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new j;onMicrotaskEmpty=new j;onStable=new j;onError=new j;run(n,t,i){return n.apply(t,i)}runGuarded(n,t,i){return n.apply(t,i)}runOutsideAngular(n){return n()}runTask(n,t,i,r){return n.apply(t,i)}};function kD(e){return dv(e,"__ignore_ng_zone__")}function TD(e){return dv(e,"__scheduler_tick__")}function dv(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var xt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Wt=new y("",{factory:()=>{let e=u(T),n=u(ye),t;return i=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw i}):(t??=n.get(xt),t.handleError(i))})}}}),uv={provide:ei,useValue:()=>{let e=u(xt,{optional:!0})},multi:!0},RD=new y("",{factory:()=>{let e=u(Y).defaultView;if(!e)return;let n=u(Wt),t=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{e.addEventListener("unhandledrejection",t),e.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(Ze).onDestroy(()=>{e.removeEventListener("error",i),e.removeEventListener("unhandledrejection",t)})}});function gf(){return $i([Rg(()=>{u(RD)})])}function O(e,n){let[t,i,r]=Wd(e,n?.equal),o=t,s=o[Ve];return o.set=i,o.update=r,o.asReadonly=vf.bind(o),o}function vf(){let e=this[Ve];if(e.readonlyFn===void 0){let n=()=>this();n[Ve]=e,e.readonlyFn=n}return e.readonlyFn}var Or=(()=>{class e{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=AD}return e})();function AD(){return new Or(K(),Ke())}var Mn=class{},Qo=new y("",{factory:()=>!0});var _f=new y("");var Ic=(()=>{class e{static \u0275prov=w({token:e,providedIn:"root",factory:()=>new Eu})}return e})(),Eu=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,i=this.queues.get(t);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,i]of this.queues)t===null?n||=this.flushQueue(i):n||=t.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},oc=class{[Ve];constructor(n){this[Ve]=n}destroy(){this[Ve].destroy()}};function On(e,n){let t=n?.injector??u(J),i=n?.manualCleanup!==!0?t.get(Ze):null,r,o=t.get(Or,null,{optional:!0}),s=t.get(Mn);return o!==null?(r=FD(o.view,s,e),i instanceof ic&&i._lView===o.view&&(i=null)):r=PD(e,t.get(Ic),s),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new oc(r)}var fv=R(_({},Yd),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Lo(!1);try{Qd(this)}finally{Lo(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=S(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],S(e)}}}),ND=R(_({},fv),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),OD=R(_({},fv),{consumerMarkedDirty(){this.view[L]|=8192,Zi(this.view),this.notifier.notify(13)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[Zn]?.delete(this)}});function FD(e,n,t){let i=Object.create(OD);return i.view=e,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=mv(i,t),e[Zn]??=new Set,e[Zn].add(i),i.consumerMarkedDirty(i),i}function PD(e,n,t){let i=Object.create(ND);return i.fn=mv(i,e),i.scheduler=n,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function mv(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function ss(e){return{toString:e}.toString()}function GD(e){return typeof e=="function"}function Yv(e,n,t,i){n!==null?n.applyValueToInputSignal(n,i):e[t]=i}var Oc=class{previousValue;currentValue;firstChange;constructor(n,t,i){this.previousValue=n,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},Oe=(()=>{let e=()=>Qv;return e.ngInherit=!0,e})();function Qv(e){return e.type.prototype.ngOnChanges&&(e.setInput=qD),WD}function WD(){let e=Zv(this),n=e?.current;if(n){let t=e.previous;if(t===Jn)e.previous=n;else for(let i in n)t[i]=n[i];e.current=null,this.ngOnChanges(n)}}function qD(e,n,t,i,r){let o=this.declaredInputs[i],s=Zv(e)||YD(e,{previous:Jn,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Oc(l&&l.currentValue,t,c===Jn),Yv(e,n,r,t)}var Kv="__ngSimpleChanges__";function Zv(e){return e[Kv]||null}function YD(e,n){return e[Kv]=n}var hv=[];var fe=function(e,n=null,t){for(let i=0;i<hv.length;i++){let r=hv[i];r(e,n,t)}},se=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(se||{});function QD(e,n,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=Qv(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}r&&(t.preOrderHooks??=[]).push(0-e,r),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function Xv(e,n){for(let t=n.directiveStart,i=n.directiveEnd;t<i;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function Tc(e,n,t){Jv(e,n,3,t)}function Rc(e,n,t,i){(e[L]&3)===t&&Jv(e,n,t,i)}function yf(e,n){let t=e[L];(t&3)===n&&(t&=16383,t+=1,e[L]=t)}function Jv(e,n,t,i){let r=i!==void 0?e[qi]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(e[qi]+=65536),(a<o||o==-1)&&(KD(e,t,n,c),e[qi]=(e[qi]&4294901760)+c+2),c++}function pv(e,n){fe(se.LifecycleHookStart,e,n);let t=S(null);try{n.call(e)}finally{S(t),fe(se.LifecycleHookEnd,e,n)}}function KD(e,n,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=e[s];r?e[L]>>14<e[qi]>>16&&(e[L]&3)===n&&(e[L]+=16384,pv(a,o)):pv(a,o)}var Pr=-1,tr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function ZD(e){return(e.flags&8)!==0}function XD(e){return(e.flags&16)!==0}function JD(e,n,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];e.setAttribute(n,s,a,o)}else{let o=r,s=t[++i];ex(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),i++}}return i}function e_(e){return e===3||e===4||e===6}function ex(e){return e.charCodeAt(0)===64}function Lr(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?gv(e,t,r,null,n[++i]):gv(e,t,r,null,null))}}return e}function gv(e,n,t,i,r){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){r!==null&&(e[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),r!==null&&e.splice(o++,0,r)}function t_(e){return e!==Pr}function Fc(e){return e&32767}function tx(e){return e>>16}function Pc(e,n){let t=tx(e),i=n;for(;t>0;)i=i[Wi],t--;return i}var Tf=!0;function vv(e){let n=Tf;return Tf=e,n}var nx=256,n_=nx-1,i_=5,ix=0,cn={};function rx(e,n,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(zi)&&(i=t[zi]),i==null&&(i=t[zi]=ix++);let r=i&n_,o=1<<r;n.data[e+(r>>i_)]|=o}function Lc(e,n){let t=r_(e,n);if(t!==-1)return t;let i=n[A];i.firstCreatePass&&(e.injectorIndex=n.length,bf(i.data,e),bf(n,null),bf(i.blueprint,null));let r=fm(e,n),o=e.injectorIndex;if(t_(r)){let s=Fc(r),a=Pc(r,n),c=a[A].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function bf(e,n){e.push(0,0,0,0,0,0,0,0,n)}function r_(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function fm(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,i=null,r=n;for(;r!==null;){if(i=l_(r),i===null)return Pr;if(t++,r=r[Wi],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Pr}function Rf(e,n,t){rx(e,n,t)}function ox(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let i=t.length,r=0;for(;r<i;){let o=t[r];if(e_(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof t[r]=="string";)r++;else{if(o===n)return t[r+1];r=r+2}}}return null}function o_(e,n,t){if(t&8||e!==void 0)return e;dc(n,"NodeInjector")}function s_(e,n,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=e[Tn],o=mt(void 0);try{return r?r.get(n,i,t&8):Pu(n,i,t&8)}finally{mt(o)}}return o_(i,n,t)}function a_(e,n,t,i=0,r){if(e!==null){if(n[L]&2048&&!(i&2)){let s=lx(e,n,t,i,cn);if(s!==cn)return s}let o=c_(e,n,t,i,cn);if(o!==cn)return o}return s_(n,t,i,r)}function c_(e,n,t,i,r){let o=ax(t);if(typeof o=="function"){if(!df(n,e,i))return i&1?o_(r,t,i):s_(n,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))dc(t);else return s}finally{uf()}}else if(typeof o=="number"){let s=null,a=r_(e,n),c=Pr,l=i&1?n[at][st]:null;for((a===-1||i&4)&&(c=a===-1?fm(e,n):n[a+8],c===Pr||!yv(i,!1)?a=-1:(s=n[A],a=Fc(c),n=Pc(c,n)));a!==-1;){let d=n[A];if(_v(o,a,d.data)){let f=sx(a,n,t,s,i,l);if(f!==cn)return f}c=n[a+8],c!==Pr&&yv(i,n[A].data[a+8]===l)&&_v(o,a,n)?(s=d,a=Fc(c),n=Pc(c,n)):a=-1}}return r}function sx(e,n,t,i,r,o){let s=n[A],a=s.data[e+8],c=i==null?Nn(a)&&Tf:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Ac(a,s,t,c,l);return d!==null?Jo(n,s,d,a,r):cn}function Ac(e,n,t,i,r){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,f=i?a:a+d,m=r?a+d:l;for(let h=f;h<m;h++){let g=s[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=s[c];if(h&&sn(h)&&h.type===t)return c}return null}function Jo(e,n,t,i,r){let o=e[t],s=n.data;if(o instanceof tr){let a=o;if(a.resolving)throw Fu("");let c=vv(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,f=a.injectImpl?mt(a.injectImpl):null,m=df(e,i,0);try{o=e[t]=a.factory(void 0,r,s,e,i),n.firstCreatePass&&t>=i.directiveStart&&QD(t,s[t],n)}finally{f!==null&&mt(f),vv(c),a.resolving=!1,uf()}}return o}function ax(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(zi)?e[zi]:void 0;return typeof n=="number"?n>=0?n&n_:cx:n}function _v(e,n,t){let i=1<<e;return!!(t[n+(e>>i_)]&i)}function yv(e,n){return!(e&2)&&!(e&1&&n)}var er=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,i){return a_(this._tNode,this._lView,n,ji(i),t)}};function cx(){return new er(Ke(),K())}function kt(e){return ss(()=>{let n=e.prototype.constructor,t=n[Fo]||Af(n),i=Object.prototype,r=Object.getPrototypeOf(e.prototype).constructor;for(;r&&r!==i;){let o=r[Fo]||Af(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Af(e){return Iu(e)?()=>{let n=Af(Ge(e));return n&&n()}:Bi(e)}function lx(e,n,t,i,r){let o=e,s=n;for(;o!==null&&s!==null&&s[L]&2048&&!Ar(s);){let a=c_(o,s,t,i|2,cn);if(a!==cn)return a;let c=o.parent;if(!c){let l=s[$u];if(l){let d=l.get(t,cn,i&-5);if(d!==cn)return d}c=l_(s),s=s[Wi]}o=c}return r}function l_(e){let n=e[A],t=n.type;return t===2?n.declTNode:t===1?e[st]:null}function Kc(e){return ox(Ke(),e)}function dx(){return Hr(Ke(),K())}function Hr(e,n){return new $(Gt(e,n))}var $=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=dx}return e})();function d_(e){return e instanceof $?e.nativeElement:e}function ux(){return this._results[Symbol.iterator]()}var si=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let i=Mg(n);(this._changesDetected=!Sg(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=ux};function u_(e){return(e.flags&128)===128}var mm=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(mm||{}),f_=new Map,fx=0;function mx(){return fx++}function hx(e){f_.set(e[Rn],e)}function Nf(e){f_.delete(e[Rn])}var bv="__ngContext__";function Vr(e,n){An(n)?(e[bv]=n[Rn],hx(n)):e[bv]=n}function m_(e){return p_(e[Rr])}function h_(e){return p_(e[Et])}function p_(e){for(;e!==null&&!$t(e);)e=e[Et];return e}var px;function hm(e){px=e}var Ur=new y("",{factory:()=>gx}),gx="ng";var Zc=new y(""),or=new y("",{providedIn:"platform",factory:()=>"unknown"}),as=new y(""),zr=new y("",{factory:()=>u(Y).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var g_="r";var v_="di";var __=!1,y_=new y("",{factory:()=>__});var wv=new WeakMap;function vx(e,n){if(e==null||typeof e!="object")return;let t=wv.get(e);t||(t=new WeakSet,wv.set(e,t)),t.add(n)}var _x=(e,n,t,i)=>{};function yx(e,n,t,i){_x(e,n,t,i)}function Xc(e){return(e.flags&32)===32}var bx=()=>null;function b_(e,n,t=!1){return bx(e,n,t)}function w_(e,n){let t=e.contentQueries;if(t!==null){let i=S(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=e.data[s];Yo(o),a.contentQueries(2,n[s],s)}}}finally{S(i)}}}function Of(e,n,t){Yo(0);let i=S(null);try{n(e,t)}finally{S(i)}}function C_(e,n,t){if(Wu(n)){let i=S(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{S(i)}}}var Qt=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(Qt||{});var Sc;function wx(){if(Sc===void 0&&(Sc=null,Sr.trustedTypes))try{Sc=Sr.trustedTypes.createPolicy("angular",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return Sc}function Jc(e){return wx()?.createHTML(e)||e}var Fn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${sc})`}},Ff=class extends Fn{getTypeName(){return"HTML"}},Pf=class extends Fn{getTypeName(){return"Style"}},Lf=class extends Fn{getTypeName(){return"Script"}},Vf=class extends Fn{getTypeName(){return"URL"}},jf=class extends Fn{getTypeName(){return"ResourceURL"}};function fn(e){return e instanceof Fn?e.changingThisBreaksApplicationSecurity:e}function sr(e,n){let t=D_(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${sc})`)}return t===n}function D_(e){return e instanceof Fn&&e.getTypeName()||null}function pm(e){return new Ff(e)}function gm(e){return new Pf(e)}function vm(e){return new Lf(e)}function _m(e){return new Vf(e)}function ym(e){return new jf(e)}function Cx(e){let n=new Hf(e);return Dx()?new Bf(n):n}var Bf=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let t=new window.DOMParser().parseFromString(Jc(n),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(n):(t.firstChild?.remove(),t)}catch{return null}}},Hf=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let t=this.inertDocument.createElement("template");return t.innerHTML=Jc(n),t}};function Dx(){try{return!!new window.DOMParser().parseFromString(Jc(""),"text/html")}catch{return!1}}var xx=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function el(e){return e=String(e),e.match(xx)?e:"unsafe:"+e}function Ln(e){let n={};for(let t of e.split(","))n[t]=!0;return n}function cs(...e){let n={};for(let t of e)for(let i in t)t.hasOwnProperty(i)&&(n[i]=!0);return n}var x_=Ln("area,br,col,hr,img,wbr"),E_=Ln("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),I_=Ln("rp,rt"),Ex=cs(I_,E_),Ix=cs(E_,Ln("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),Sx=cs(I_,Ln("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Cv=cs(x_,Ix,Sx,Ex),S_=Ln("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),Mx=Ln("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),kx=Ln("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),Tx=cs(S_,Mx,kx),Rx=Ln("script,style,template");var Uf=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let t=n.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=Ox(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let o=Nx(t);if(o){t=o;break}t=r.pop()}}return this.buf.join("")}startElement(n){let t=Dv(n).toLowerCase();if(!Cv.hasOwnProperty(t))return this.sanitizedSomething=!0,!Rx.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!Tx.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;S_[a]&&(c=el(c)),this.buf.push(" ",s,'="',xv(c),'"')}return this.buf.push(">"),!0}endElement(n){let t=Dv(n).toLowerCase();Cv.hasOwnProperty(t)&&!x_.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(n){this.buf.push(xv(n))}};function Ax(e,n){return(e.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function Nx(e){let n=e.nextSibling;if(n&&e!==n.previousSibling)throw M_(n);return n}function Ox(e){let n=e.firstChild;if(n&&Ax(e,n))throw M_(n);return n}function Dv(e){let n=e.nodeName;return typeof n=="string"?n:"FORM"}function M_(e){return new Error(`Failed to sanitize html because the element is clobbered: ${e.outerHTML}`)}var Fx=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,Px=/([^\#-~ |!])/g;function xv(e){return e.replace(/&/g,"&amp;").replace(Fx,function(n){let t=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(Px,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Mc;function bm(e,n){let t=null;try{Mc=Mc||Cx(e);let i=n?String(n):"";t=Mc.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=t.innerHTML,t=Mc.getInertBodyElement(i)}while(i!==o);let a=new Uf().sanitizeChildren(Ev(t)||t);return Jc(a)}finally{if(t){let i=Ev(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function Ev(e){return"content"in e&&Lx(e)?e.content:null}function Lx(e){return e.nodeType===Node.ELEMENT_NODE&&e.nodeName==="TEMPLATE"}function Vx(e,n){return e.createText(n)}function jx(e,n,t){e.setValue(n,t)}function k_(e,n,t){return e.createElement(n,t)}function Vc(e,n,t,i,r){e.insertBefore(n,t,i,r)}function T_(e,n,t){e.appendChild(n,t)}function Iv(e,n,t,i,r){i!==null?Vc(e,n,t,i,r):T_(e,n,t)}function R_(e,n,t,i){e.removeChild(null,n,t,i)}function Bx(e,n,t){e.setAttribute(n,"style",t)}function Hx(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function A_(e,n,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&JD(e,n,i),r!==null&&Hx(e,n,r),o!==null&&Bx(e,n,o)}var Kt=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e})(Kt||{});function N_(e){return e instanceof Function?e():e}function Ux(e,n,t){let i=e.length;for(;;){let r=e.indexOf(n,t);if(r===-1)return r;if(r===0||e.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||e.charCodeAt(r+o)<=32)return r}t=r+1}}var O_="ng-template";function zx(e,n,t,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&Ux(n[r+1].toLowerCase(),t,0)!==-1)return!0}else if(wm(e))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function wm(e){return e.type===4&&e.value!==O_}function $x(e,n,t){let i=e.type===4&&!t?O_:e.value;return n===i}function Gx(e,n,t){let i=4,r=e.attrs,o=r!==null?Yx(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!qt(i)&&!qt(c))return!1;if(s&&qt(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!$x(e,c,t)||c===""&&n.length===1){if(qt(i))return!1;s=!0}}else if(i&8){if(r===null||!zx(e,r,c,t)){if(qt(i))return!1;s=!0}}else{let l=n[++a],d=Wx(c,r,wm(e),t);if(d===-1){if(qt(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(qt(i))return!1;s=!0}}}}return qt(i)||s}function qt(e){return(e&1)===0}function Wx(e,n,t,i){if(n===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<n.length;){let s=n[r];if(s===e)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return Qx(n,e)}function F_(e,n,t=!1){for(let i=0;i<n.length;i++)if(Gx(e,n[i],t))return!0;return!1}function qx(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function Yx(e){for(let n=0;n<e.length;n++){let t=e[n];if(e_(t))return n}return e.length}function Qx(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let i=e[t];if(typeof i=="number")return-1;if(i===n)return t;t++}return-1}function Kx(e,n){e:for(let t=0;t<n.length;t++){let i=n[t];if(e.length===i.length){for(let r=0;r<e.length;r++)if(e[r]!==i[r])continue e;return!0}}return!1}function Sv(e,n){return e?":not("+n.trim()+")":n}function Zx(e){let n=e[0],t=1,i=2,r="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(i&2){let a=e[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!qt(s)&&(n+=Sv(o,r),r=""),i=s,o=o||!qt(i);t++}return r!==""&&(n+=Sv(o,r)),n}function Xx(e){return e.map(Zx).join(",")}function Jx(e){let n=[],t=[],i=1,r=2;for(;i<e.length;){let o=e[i];if(typeof o=="string")r===2?o!==""&&n.push(o,e[++i]):r===8&&t.push(o);else{if(!qt(r))break;r=o}i++}return t.length&&n.push(1,...t),n}var _t={};function Cm(e,n,t,i,r,o,s,a,c,l,d){let f=ke+i,m=f+r,h=eE(f,m),g=typeof l=="function"?l():l;return h[A]={type:e,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:n,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:d}}function eE(e,n){let t=[];for(let i=0;i<n;i++)t.push(i<e?null:_t);return t}function tE(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=Cm(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function Dm(e,n,t,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[zt]=r,f[L]=i|4|128|8|64|1024,(l!==null||e&&e[L]&2048)&&(f[L]|=2048),Qu(f),f[Be]=f[Wi]=e,f[Me]=t,f[rn]=s||e&&e[rn],f[Ce]=a||e&&e[Ce],f[Tn]=c||e&&e[Tn]||null,f[st]=o,f[Rn]=mx(),f[Gi]=d,f[$u]=l,f[at]=n.type==2?e[at]:f,f}function nE(e,n,t){let i=Gt(n,e),r=tE(t),o=e[rn].rendererFactory,s=xm(e,Dm(e,r,null,P_(t),i,n,null,o.createRenderer(i,t),null,null,null));return e[n.index]=s}function P_(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function L_(e,n,t,i){if(t===0)return-1;let r=n.length;for(let o=0;o<t;o++)n.push(i),e.blueprint.push(i),e.data.push(null);return r}function xm(e,n){return e[Rr]?e[zu][Et]=n:e[Rr]=n,e[zu]=n,n}function b(e=1){V_(Ae(),K(),an()+e,!1)}function V_(e,n,t,i){if(!i)if((n[L]&3)===3){let o=e.preOrderCheckHooks;o!==null&&Tc(n,o,t)}else{let o=e.preOrderHooks;o!==null&&Rc(n,o,0,t)}ri(t)}var tl=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(tl||{});function zf(e,n,t,i){let r=S(null);try{let[o,s,a]=e.inputs[t],c=null;(s&tl.SignalBased)!==0&&(c=n[o][Ve]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),e.setInput!==null?e.setInput(n,c,i,t,o):Yv(n,c,o,i)}finally{S(r)}}var ln=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(ln||{}),iE;function Em(e,n){return iE(e,n)}var _2=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var $f=new WeakMap,Ko=new WeakSet;function rE(e,n){let t=$f.get(e);if(!t||t.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===n?(t.splice(o,1),Ko.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function oE(e,n){let t=$f.get(e);t?t.includes(n)||t.push(n):$f.set(e,[n])}var nr=new Set,nl=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(nl||{}),Vn=new y(""),Mv=new Set;function jn(e){Mv.has(e)||(Mv.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var il=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:e,providedIn:"root",factory:()=>new e})}return e})(),Im=[0,1,2,3],Sm=(()=>{class e{ngZone=u(T);scheduler=u(Mn);errorHandler=u(xt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(Vn,{optional:!0})}execute(){let t=this.sequences.size>0;t&&fe(se.AfterRenderHooksStart),this.executing=!0;for(let i of Im)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&fe(se.AfterRenderHooksEnd)}register(t){let{view:i}=t;i!==void 0?((i[Yi]??=[]).push(t),Zi(i),i[L]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(nl.AFTER_NEXT_RENDER,t):t()}static \u0275prov=w({token:e,providedIn:"root",factory:()=>new e})}return e})(),es=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,i,r,o,s=null){this.impl=n,this.hooks=t,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Yi];n&&(this.view[Yi]=n.filter(t=>t!==this))}};function Tt(e,n){let t=n?.injector??u(J);return jn("NgAfterNextRender"),aE(e,t,n,!0)}function sE(e){return e instanceof Function?[void 0,void 0,e,void 0]:[e.earlyRead,e.write,e.mixedReadWrite,e.read]}function aE(e,n,t,i){let r=n.get(il);r.impl??=n.get(Sm);let o=n.get(Vn,null,{optional:!0}),s=t?.manualCleanup!==!0?n.get(Ze):null,a=n.get(Or,null,{optional:!0}),c=new es(r.impl,sE(e),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var j_=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:u(ye)})});function B_(e,n,t){let i=e.get(j_);if(Array.isArray(n))for(let r of n)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(e)}function cE(e,n){let t=e.get(j_);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)t.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function lE(e,n){for(let[t,i]of n)B_(e,i.animateFns)}function kv(e,n,t,i){let r=e?.[ni]?.enter;n!==null&&r&&r.has(t.index)&&lE(i,r)}function Fr(e,n,t,i,r,o,s,a){if(r!=null){let c,l=!1;$t(r)?c=r:An(r)&&(l=!0,r=r[zt]);let d=It(r);e===0&&i!==null?(kv(a,i,o,t),s==null?T_(n,i,d):Vc(n,i,d,s||null,!0)):e===1&&i!==null?(kv(a,i,o,t),Vc(n,i,d,s||null,!0),rE(o,d)):e===2?(a?.[ni]?.leave?.has(o.index)&&oE(o,d),Ko.delete(d),Tv(a,o,t,f=>{if(Ko.has(d)){Ko.delete(d);return}R_(n,d,l,f)})):e===3&&(Ko.delete(d),Tv(a,o,t,()=>{n.destroyNode(d)})),c!=null&&bE(n,e,t,c,o,i,s)}}function dE(e,n){H_(e,n),n[zt]=null,n[st]=null}function uE(e,n,t,i,r,o){i[zt]=r,i[st]=n,ol(e,i,t,1,r,o)}function H_(e,n){n[rn].changeDetectionScheduler?.notify(9),ol(e,n,n[Ce],2,null,null)}function fE(e){let n=e[Rr];if(!n)return wf(e[A],e);for(;n;){let t=null;if(An(n))t=n[Rr];else{let i=n[Ie];i&&(t=i)}if(!t){for(;n&&!n[Et]&&n!==e;)An(n)&&wf(n[A],n),n=n[Be];n===null&&(n=e),An(n)&&wf(n[A],n),t=n&&n[Et]}n=t}}function Mm(e,n){let t=e[Qi],i=t.indexOf(n);t.splice(i,1)}function rl(e,n){if(Ki(n))return;let t=n[Ce];t.destroyNode&&ol(e,n,t,3,null,null),fE(n)}function wf(e,n){if(Ki(n))return;let t=S(null);try{n[L]&=-129,n[L]|=256,n[pt]&&Kn(n[pt]),pE(e,n),hE(e,n),n[A].type===1&&n[Ce].destroy();let i=n[ti];if(i!==null&&$t(n[Be])){i!==n[Be]&&Mm(i,n);let r=n[on];r!==null&&r.detachView(e)}Nf(n)}finally{S(t)}}function Tv(e,n,t,i){let r=e?.[ni];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);e&&nr.add(e[Rn]),B_(t,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),mE(e,i)}else e&&nr.delete(e[Rn]),i(!1)},r)}function mE(e,n){let t=e[ni]?.running;if(t){t.then(()=>{e[ni].running=void 0,nr.delete(e[Rn]),n(!0)});return}n(!1)}function hE(e,n){let t=e.cleanup,i=n[Tr];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(n[Tr]=null);let r=n[Sn];if(r!==null){n[Sn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Zn];if(o!==null){n[Zn]=null;for(let s of o)s.destroy()}}function pE(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=n[t[i]];if(!(r instanceof tr)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];fe(se.LifecycleHookStart,a,c);try{c.call(a)}finally{fe(se.LifecycleHookEnd,a,c)}}else{fe(se.LifecycleHookStart,r,o);try{o.call(r)}finally{fe(se.LifecycleHookEnd,r,o)}}}}}function U_(e,n,t){return gE(e,n.parent,t)}function gE(e,n,t){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return t[zt];if(Nn(i)){let{encapsulation:r}=e.data[i.directiveStart+i.componentOffset];if(r===Qt.None||r===Qt.Emulated)return null}return Gt(i,t)}function z_(e,n,t){return _E(e,n,t)}function vE(e,n,t){return e.type&40?Gt(e,t):null}var _E=vE,Rv;function km(e,n,t,i){let r=U_(e,i,n),o=n[Ce],s=i.parent||n[st],a=z_(s,i,n);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Iv(o,r,t[c],a,!1);else Iv(o,r,t,a,!1);Rv!==void 0&&Rv(o,i,n,t,r)}function Zo(e,n){if(n!==null){let t=n.type;if(t&3)return Gt(n,e);if(t&4)return Gf(-1,e[n.index]);if(t&8){let i=n.child;if(i!==null)return Zo(e,i);{let r=e[n.index];return $t(r)?Gf(-1,r):It(r)}}else{if(t&128)return Zo(e,n.next);if(t&32)return Em(n,e)()||It(e[n.index]);{let i=$_(e,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Xn(e[at]);return Zo(r,i)}else return Zo(e,n.next)}}}return null}function $_(e,n){if(n!==null){let i=e[at][st],r=n.projection;return i.projection[r]}return null}function Gf(e,n){let t=Ie+e+1;if(t<n.length){let i=n[t],r=i[A].firstChild;if(r!==null)return Zo(i,r)}return n[ii]}function Tm(e,n,t,i,r,o,s){for(;t!=null;){let a=i[Tn];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&n===0&&(c&&Vr(It(c),i),t.flags|=2),!Xc(t))if(l&8)Tm(e,n,t.child,i,r,o,!1),Fr(n,e,a,r,c,t,o,i);else if(l&32){let d=Em(t,i),f;for(;f=d();)Fr(n,e,a,r,f,t,o,i);Fr(n,e,a,r,c,t,o,i)}else l&16?G_(e,n,i,t,r,o):Fr(n,e,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function ol(e,n,t,i,r,o){Tm(t,i,e.firstChild,n,r,o,!1)}function yE(e,n,t){let i=n[Ce],r=U_(e,t,n),o=t.parent||n[st],s=z_(o,t,n);G_(i,0,n,t,r,s)}function G_(e,n,t,i,r,o){let s=t[at],c=s[st].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Fr(n,e,t[Tn],r,d,i,o,t)}else{let l=c,d=s[Be];u_(i)&&(l.flags|=128),Tm(e,n,l,d,r,o,!0)}}function bE(e,n,t,i,r,o,s){let a=i[ii],c=It(i);a!==c&&Fr(n,e,t,o,a,r,s);for(let l=Ie;l<i.length;l++){let d=i[l];ol(d[A],d,e,n,o,a)}}function wE(e,n,t,i,r){if(n)r?e.addClass(t,i):e.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:ln.DashCase;r==null?e.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=ln.Important),e.setStyle(t,i,r,o))}}function W_(e,n,t,i,r){let o=an(),s=i&2;try{ri(-1),s&&n.length>ke&&V_(e,n,ke,!1);let a=s?se.TemplateUpdateStart:se.TemplateCreateStart;fe(a,r,t),t(i,r)}finally{ri(o);let a=s?se.TemplateUpdateEnd:se.TemplateCreateEnd;fe(a,r,t)}}function Rm(e,n,t){ME(e,n,t),(t.flags&64)===64&&kE(e,n,t)}function sl(e,n,t=Gt){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(n,e):e[s];e[r++]=a}}}function CE(e,n,t,i){let o=i.get(y_,__)||t===Qt.ShadowDom||t===Qt.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);return DE(s),s}function DE(e){xE(e)}var xE=()=>null;function EE(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function IE(e,n,t,i,r,o){let s=n[A];if(Am(e,s,n,t,i)){Nn(e)&&SE(n,e.index);return}e.type&3&&(t=EE(t)),q_(e,n,t,i,r,o)}function q_(e,n,t,i,r,o){if(e.type&3){let s=Gt(e,n);i=o!=null?o(i,e.value||"",t):i,r.setProperty(s,t,i)}else e.type&12}function SE(e,n){let t=St(n,e);t[L]&16||(t[L]|=64)}function ME(e,n,t){let i=t.directiveStart,r=t.directiveEnd;Nn(t)&&nE(n,t,e.data[i+t.componentOffset]),e.firstCreatePass||Lc(t,n);let o=t.initialInputs;for(let s=i;s<r;s++){let a=e.data[s],c=Jo(n,e,s,t);if(Vr(c,n),o!==null&&NE(n,s-i,c,a,t,o),sn(a)){let l=St(t.index,n);l[Me]=Jo(n,e,s,t)}}}function kE(e,n,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=Jg();try{ri(o);for(let a=i;a<r;a++){let c=e.data[a],l=n[a];yc(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&TE(c,l)}}finally{ri(-1),yc(s)}}function TE(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function Y_(e,n){let t=e.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];F_(n,o.selectors,!1)&&(i??=[],sn(o)?i.unshift(o):i.push(o))}return i}function RE(e,n,t,i,r,o){let s=Gt(e,n);AE(n[Ce],s,o,e.value,t,i,r)}function AE(e,n,t,i,r,o,s){if(o==null)e.removeAttribute(n,r,t);else{let a=s==null?Uo(o):s(o,i||"",r);e.setAttribute(n,r,a,t)}}function NE(e,n,t,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];zf(i,t,c,l)}}function Q_(e,n,t,i,r){let o=ke+t,s=n[A],a=r(s,n,e,i,t);n[o]=a,Nr(e,!0);let c=e.type===2;return c?(A_(n[Ce],a,e),(Gg()===0||Wo(e))&&Vr(a,n),Wg()):Vr(a,n),xc()&&(!c||!Xc(e))&&km(s,n,a,e),e}function K_(e){let n=e;return af()?cf():(n=n.parent,Nr(n,!1)),n}function OE(e,n){let t=e[Tn];if(!t)return;let i;try{i=t.get(Wt,null)}catch{i=null}i?.(n)}function Am(e,n,t,i,r){let o=e.inputs?.[i],s=e.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];zf(f,t[l],d,r),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];zf(d,l,i,r),a=!0}return a}function FE(e,n){let t=St(n,e),i=t[A];PE(i,t);let r=t[zt];r!==null&&t[Gi]===null&&(t[Gi]=b_(r,t[Tn])),fe(se.ComponentStart);try{Nm(i,t,t[Me])}finally{fe(se.ComponentEnd,t[Me])}}function PE(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function Nm(e,n,t){wc(n);try{let i=e.viewQuery;i!==null&&Of(1,i,t);let r=e.template;r!==null&&W_(e,n,r,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[on]?.finishViewCreation(e),e.staticContentQueries&&w_(e,n),e.staticViewQueries&&Of(2,e.viewQuery,t);let o=e.components;o!==null&&LE(n,o)}catch(i){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),i}finally{n[L]&=-5,Cc()}}function LE(e,n){for(let t=0;t<n.length;t++)FE(e,n[t])}function ls(e,n,t,i){let r=S(null);try{let o=n.tView,a=e[L]&4096?4096:16,c=Dm(e,o,t,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=e[n.index];c[ti]=l;let d=e[on];return d!==null&&(c[on]=d.createEmbeddedView(o)),Nm(o,c,t),c}finally{S(r)}}function jr(e,n){return!n||n.firstChild===null||u_(e)}function ts(e,n,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=n[t.index];o!==null&&i.push(It(o)),$t(o)&&Z_(o,i);let s=t.type;if(s&8)ts(e,n,t.child,i);else if(s&32){let a=Em(t,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=$_(n,t);if(Array.isArray(a))i.push(...a);else{let c=Xn(n[at]);ts(c[A],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function Z_(e,n){for(let t=Ie;t<e.length;t++){let i=e[t],r=i[A].firstChild;r!==null&&ts(i[A],i,r,n)}e[ii]!==e[zt]&&n.push(e[ii])}function X_(e){if(e[Yi]!==null){for(let n of e[Yi])n.impl.addSequence(n);e[Yi].length=0}}var J_=[];function VE(e){return e[pt]??jE(e)}function jE(e){let n=J_.pop()??Object.create(HE);return n.lView=e,n}function BE(e){e.lView[pt]!==e&&(e.lView=null,J_.push(e))}var HE=R(_({},Mi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{Zi(e.lView)},consumerOnSignalRead(){this.lView[pt]=this}});function UE(e){let n=e[pt]??Object.create(zE);return n.lView=e,n}var zE=R(_({},Mi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Xn(e.lView);for(;n&&!ey(n[A]);)n=Xn(n);n&&Ku(n)},consumerOnSignalRead(){this.lView[pt]=this}});function ey(e){return e.type!==2}function ty(e){if(e[Zn]===null)return;let n=!0;for(;n;){let t=!1;for(let i of e[Zn])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=t&&!!(e[L]&8192)}}var $E=100;function ny(e,n=0){let i=e[rn].rendererFactory,r=!1;r||i.begin?.();try{GE(e,n)}finally{r||i.end?.()}}function GE(e,n){let t=lf();try{Lo(!0),Wf(e,n);let i=0;for(;qo(e);){if(i===$E)throw new x(103,!1);i++,Wf(e,1)}}finally{Lo(t)}}function WE(e,n,t,i){if(Ki(n))return;let r=n[L],o=!1,s=!1;wc(n);let a=!0,c=null,l=null;o||(ey(e)?(l=VE(n),c=Qn(l)):ga()===null?(a=!1,l=UE(n),c=Qn(l)):n[pt]&&(Kn(n[pt]),n[pt]=null));try{Qu(n),Kg(e.bindingStartIndex),t!==null&&W_(e,n,t,2,i);let d=(r&3)===3;if(!o)if(d){let h=e.preOrderCheckHooks;h!==null&&Tc(n,h,null)}else{let h=e.preOrderHooks;h!==null&&Rc(n,h,0,null),yf(n,0)}if(s||qE(n),ty(n),iy(n,0),e.contentQueries!==null&&w_(e,n),!o)if(d){let h=e.contentCheckHooks;h!==null&&Tc(n,h)}else{let h=e.contentHooks;h!==null&&Rc(n,h,1),yf(n,1)}QE(e,n);let f=e.components;f!==null&&oy(n,f,0);let m=e.viewQuery;if(m!==null&&Of(2,m,i),!o)if(d){let h=e.viewCheckHooks;h!==null&&Tc(n,h)}else{let h=e.viewHooks;h!==null&&Rc(n,h,2),yf(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[hc]){for(let h of n[hc])h();n[hc]=null}o||(X_(n),n[L]&=-73)}catch(d){throw o||Zi(n),d}finally{l!==null&&(Ti(l,c),a&&BE(l)),Cc()}}function iy(e,n){for(let t=m_(e);t!==null;t=h_(t))for(let i=Ie;i<t.length;i++){let r=t[i];ry(r,n)}}function qE(e){for(let n=m_(e);n!==null;n=h_(n)){if(!(n[L]&2))continue;let t=n[Qi];for(let i=0;i<t.length;i++){let r=t[i];Ku(r)}}}function YE(e,n,t){fe(se.ComponentStart);let i=St(n,e);try{ry(i,t)}finally{fe(se.ComponentEnd,i[Me])}}function ry(e,n){gc(e)&&Wf(e,n)}function Wf(e,n){let i=e[A],r=e[L],o=e[pt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&gr(o)),s||=!1,o&&(o.dirty=!1),e[L]&=-9217,s)WE(i,e,i.template,e[Me]);else if(r&8192){let a=S(null);try{ty(e),iy(e,1);let c=i.components;c!==null&&oy(e,c,1),X_(e)}finally{S(a)}}}function oy(e,n,t){for(let i=0;i<n.length;i++)YE(e,n[i],t)}function QE(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)ri(~r);else{let o=r,s=t[++i],a=t[++i];Xg(s,o);let c=n[o];fe(se.HostBindingsUpdateStart,c);try{a(2,c)}finally{fe(se.HostBindingsUpdateEnd,c)}}}}finally{ri(-1)}}function Om(e,n){let t=lf()?64:1088;for(e[rn].changeDetectionScheduler?.notify(n);e;){e[L]|=t;let i=Xn(e);if(Ar(e)&&!i)return e;e=i}return null}function sy(e,n,t,i){return[e,!0,0,n,null,i,null,t,null,null]}function ay(e,n){let t=Ie+n;if(t<e.length)return e[t]}function ds(e,n,t,i=!0){let r=n[A];if(KE(r,n,e,t),i){let s=Gf(t,e),a=n[Ce],c=a.parentNode(e[ii]);c!==null&&uE(r,e[st],a,n,c,s)}let o=n[Gi];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function cy(e,n){let t=ns(e,n);return t!==void 0&&rl(t[A],t),t}function ns(e,n){if(e.length<=Ie)return;let t=Ie+n,i=e[t];if(i){let r=i[ti];r!==null&&r!==e&&Mm(r,i),n>0&&(e[t-1][Et]=i[Et]);let o=zo(e,Ie+n);dE(i[A],i);let s=o[on];s!==null&&s.detachView(o[A]),i[Be]=null,i[Et]=null,i[L]&=-129}return i}function KE(e,n,t,i){let r=Ie+i,o=t.length;i>0&&(t[r-1][Et]=n),i<o-Ie?(n[Et]=t[r],Lu(t,Ie+i,n)):(t.push(n),n[Et]=null),n[Be]=t;let s=n[ti];s!==null&&t!==s&&ly(s,n);let a=n[on];a!==null&&a.insertView(e),vc(n),n[L]|=128}function ly(e,n){let t=e[Qi],i=n[Be];if(An(i))e[L]|=2;else{let r=i[Be][at];n[at]!==r&&(e[L]|=2)}t===null?e[Qi]=[n]:t.push(n)}var ai=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[A];return ts(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[Me]}set context(n){this._lView[Me]=n}get destroyed(){return Ki(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Be];if($t(n)){let t=n[Go],i=t?t.indexOf(this):-1;i>-1&&(ns(n,i),zo(t,i))}this._attachedToViewContainer=!1}rl(this._lView[A],this._lView)}onDestroy(n){Zu(this._lView,n)}markForCheck(){Om(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[L]&=-129}reattach(){vc(this._lView),this._lView[L]|=128}detectChanges(){this._lView[L]|=1024,ny(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ar(this._lView),t=this._lView[ti];t!==null&&!n&&Mm(t,this._lView),H_(this._lView[A],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=n;let t=Ar(this._lView),i=this._lView[ti];i!==null&&!t&&ly(i,this._lView),vc(this._lView)}};var dn=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=ZE;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=ls(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new ai(o)}}return e})();function ZE(){return al(Ke(),K())}function al(e,n){return e.type&4?new dn(n,e,Hr(e,n)):null}function $r(e,n,t,i,r){let o=e.data[n];if(o===null)o=XE(e,n,t,i,r),Zg()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=qg();o.injectorIndex=s===null?-1:s.injectorIndex}return Nr(o,!0),o}function XE(e,n,t,i,r){let o=sf(),s=af(),a=s?o:o&&o.parent,c=e.data[n]=eI(e,a,t,n,i,r);return JE(e,c,o,s),c}function JE(e,n,t,i){e.firstChild===null&&(e.firstChild=n),t!==null&&(i?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function eI(e,n,t,i,r,o){let s=n?n.injectorIndex:-1,a=0;return nf()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function tI(e){let n=e[Gu]??[],i=e[Be][Ce],r=[];for(let o of n)o.data[v_]!==void 0?r.push(o):nI(o,i);e[Gu]=r}function nI(e,n){let t=0,i=e.firstChild;if(i){let r=e.data[g_];for(;t<r;){let o=i.nextSibling;R_(n,i,!1),i=o,t++}}}var iI=()=>null,rI=()=>null;function jc(e,n){return iI(e,n)}function dy(e,n,t){return rI(e,n,t)}var uy=class{},cl=class{},qf=class{resolveComponentFactory(n){throw new x(917,!1)}},us=class{static NULL=new qf},Ne=class{},Fe=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>oI()}return e})();function oI(){let e=K(),n=Ke(),t=St(n.index,e);return(An(t)?t:e)[Ce]}var fy=(()=>{class e{static \u0275prov=w({token:e,providedIn:"root",factory:()=>null})}return e})();var Nc={},Yf=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,i){let r=this.injector.get(n,Nc,i);return r!==Nc||t===Nc?r:this.parentInjector.get(n,t,i)}};function Bc(e,n,t){let i=t?e.styles:null,r=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=ac(r,a);else if(o==2){let c=a,l=n[++s];i=ac(i,c+": "+l+";")}}t?e.styles=i:e.stylesWithoutHost=i,t?e.classes=r:e.classesWithoutHost=r}function dt(e,n=0){let t=K();if(t===null)return z(e,n);let i=Ke();return a_(i,t,Ge(e),n)}function Fm(){let e="invalid";throw new Error(e)}function my(e,n,t,i,r){let o=i===null?null:{"":-1},s=r(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}cI(e,n,t,a,o,c,l)}o!==null&&i!==null&&sI(t,i,o)}function sI(e,n,t){let i=e.localNames=[];for(let r=0;r<n.length;r+=2){let o=t[n[r+1]];if(o==null)throw new x(-301,!1);i.push(n[r],o)}}function aI(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function cI(e,n,t,i,r,o,s){let a=i.length,c=null;for(let m=0;m<a;m++){let h=i[m];c===null&&sn(h)&&(c=h,aI(e,t,m)),Rf(Lc(t,n),e,h.type)}hI(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let h=i[m];h.providersResolver&&h.providersResolver(h)}let l=!1,d=!1,f=L_(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let m=0;m<a;m++){let h=i[m];if(t.mergedAttrs=Lr(t.mergedAttrs,h.hostAttrs),dI(e,t,n,f,h),mI(f,h,r),s!==null&&s.has(h)){let[C,W]=s.get(h);t.directiveToIndex.set(h.type,[f,C+t.directiveStart,W+t.directiveStart])}else(o===null||!o.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(g.ngOnChanges||g.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),f++}lI(e,t,o)}function lI(e,n,t){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=e.data[i];if(t===null||!t.has(r))Av(0,n,r,i),Av(1,n,r,i),Ov(n,i,!1);else{let o=t.get(r);Nv(0,n,o,i),Nv(1,n,o,i),Ov(n,i,!0)}}}function Av(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),hy(n,o)}}function Nv(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),hy(n,s)}}function hy(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function Ov(e,n,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=e;if(i===null||!t&&r===null||t&&o===null||wm(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function dI(e,n,t,i,r){e.data[i]=r;let o=r.factory||(r.factory=Bi(r.type,!0)),s=new tr(o,sn(r),dt,null);e.blueprint[i]=s,t[i]=s,uI(e,n,i,L_(e,t,r.hostVars,_t),r)}function uI(e,n,t,i,r){let o=r.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;fI(s)!=a&&s.push(a),s.push(t,i,o)}}function fI(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function mI(e,n,t){if(t){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)t[n.exportAs[i]]=e;sn(n)&&(t[""]=e)}}function hI(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function py(e,n,t,i,r,o,s,a){let c=n[A],l=c.consts,d=Mt(l,s),f=$r(c,e,t,i,d);return o&&my(c,n,f,Mt(l,a),r),f.mergedAttrs=Lr(f.mergedAttrs,f.attrs),f.attrs!==null&&Bc(f,f.attrs,!1),f.mergedAttrs!==null&&Bc(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function gy(e,n){Xv(e,n),Wu(n)&&e.queries.elementEnd(n)}function pI(e,n,t,i,r,o){let s=n.consts,a=Mt(s,r),c=$r(n,e,t,i,a);if(c.mergedAttrs=Lr(c.mergedAttrs,c.attrs),o!=null){let l=Mt(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Bc(c,c.attrs,!1),c.mergedAttrs!==null&&Bc(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function un(e,n,t){if(t===_t)return!1;let i=e[n];return Object.is(i,t)?!1:(e[n]=t,!0)}function gI(e,n,t,i){let r=un(e,n,t);return un(e,n+1,i)||r}function Cf(e,n,t){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&vx(r,o);let s=Nn(e)?St(e.index,n):n;Om(s,5);let a=n[Me],c=Fv(n,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=Fv(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Fv(e,n,t,i){let r=S(null);try{return fe(se.OutputStart,n,t),t(i)!==!1}catch(o){return OE(e,o),!1}finally{fe(se.OutputEnd,n,t),S(r)}}function vI(e,n,t,i,r,o,s,a){let c=Wo(e),l=!1,d=null;if(!i&&c&&(d=yI(n,t,o,e.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=Gt(e,t),m=i?i(f):f;yx(t,m,o,a),i||(a.__ngNativeEl__=f);let h=r.listen(m,o,a);if(!_I(o)){let g=i?C=>i(It(C[e.index])):e.index;vy(g,n,t,o,a,h,!1)}}return l}function _I(e){return e.startsWith("animation")||e.startsWith("transition")}function yI(e,n,t,i){let r=e.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=n[Tr],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function vy(e,n,t,i,r,o,s){let a=n.firstCreatePass?Ju(n):null,c=Xu(t),l=c.length;c.push(r,o),a&&a.push(i,e,l,(l+1)*(s?-1:1))}function Pv(e,n,t,i,r,o){let s=n[t],a=n[A],l=a.data[t].outputs[i],f=s[l].subscribe(o);vy(e.index,a,n,r,o,f,!0)}var Qf=Symbol("BINDING");function _y(e){return e.debugInfo?.className||e.type.name||null}var Hc=class extends us{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let t=kn(n);return new ir(t,this.ngModule)}};function bI(e){return Object.keys(e).map(n=>{let[t,i,r]=e[n],o={propName:t,templateName:n,isSignal:(i&tl.SignalBased)!==0};return r&&(o.transform=r),o})}function wI(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function CI(e,n,t){let i=n instanceof ye?n:n?.injector;return i&&e.getStandaloneInjector!==null&&(i=e.getStandaloneInjector(i)||i),i?new Yf(t,i):t}function DI(e){let n=e.get(Ne,null);if(n===null)throw new x(407,!1);let t=e.get(fy,null),i=e.get(Mn,null),r=e.get(Vn,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function xI(e,n){let t=yy(e);return k_(n,t,t==="svg"?qu:t==="math"?Vg:null)}function yy(e){return(e.selectors[0][0]||"div").toLowerCase()}var ir=class extends cl{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=bI(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=wI(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){super(),this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=Xx(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,i,r,o,s){fe(se.DynamicComponentStart);let a=S(null);try{let c=this.componentDef,l=CI(c,r||this.ngModule,n),d=DI(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(_y(c),()=>this.createComponentRef(d,l,t,i,o,s)):this.createComponentRef(d,l,t,i,o,s)}finally{S(a)}}createComponentRef(n,t,i,r,o,s){let a=this.componentDef,c=EI(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?CE(l,r,a.encapsulation,t):xI(a,l),f=s?.some(Lv)||o?.some(g=>typeof g!="function"&&g.bindings.some(Lv)),m=Dm(null,c,null,512|P_(a),null,null,n,l,t,null,b_(d,t,!0));m[ke]=d,wc(m);let h=null;try{let g=py(ke,m,2,"#host",()=>c.directiveRegistry,!0,0);A_(l,d,g),Vr(d,m),Rm(c,m,g),C_(c,g,m),gy(c,g),i!==void 0&&SI(g,this.ngContentSelectors,i),h=St(g.index,m),m[Me]=h[Me],Nm(c,m,null)}catch(g){throw h!==null&&Nf(h),Nf(m),g}finally{fe(se.DynamicComponentEnd),Cc()}return new Uc(this.componentType,m,!!f)}};function EI(e,n,t,i){let r=e?["ng-version","21.2.12"]:Jx(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[Qf].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let m of f.bindings){a+=m[Qf].requiredVars;let h=d+1;m.create&&(m.targetIdx=h,(o??=[]).push(m)),m.update&&(m.targetIdx=h,(s??=[]).push(m))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,m=Ou(f);c.push(m)}return Cm(0,null,II(o,s),1,a,c,null,null,null,[r],null)}function II(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let i of e)i.create();if(t&2&&n)for(let i of n)i.update()}}function Lv(e){let n=e[Qf].kind;return n==="input"||n==="twoWay"}var Uc=class extends uy{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=pc(t[A],ke),this.location=Hr(this._tNode,t),this.instance=St(this._tNode.index,t)[Me],this.hostView=this.changeDetectorRef=new ai(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let r=this._rootLView,o=Am(i,r[A],r,n,t);this.previousInputValues.set(n,t);let s=St(i.index,r);Om(s,1)}get injector(){return new er(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function SI(e,n,t){let i=e.projection=[];for(let r=0;r<n.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Rt=(()=>{class e{static __NG_ELEMENT_ID__=MI}return e})();function MI(){let e=Ke();return by(e,K())}var Kf=class e extends Rt{_lContainer;_hostTNode;_hostLView;constructor(n,t,i){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=i}get element(){return Hr(this._hostTNode,this._hostLView)}get injector(){return new er(this._hostTNode,this._hostLView)}get parentInjector(){let n=fm(this._hostTNode,this._hostLView);if(t_(n)){let t=Pc(n,this._hostLView),i=Fc(n),r=t[A].data[i+8];return new er(r,t)}else return new er(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=Vv(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-Ie}createEmbeddedView(n,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=jc(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,jr(this._hostTNode,s)),a}createComponent(n,t,i,r,o,s,a){let c=n&&!GD(n),l;if(c)l=t;else{let W=t||{};l=W.index,i=W.injector,r=W.projectableNodes,o=W.environmentInjector||W.ngModuleRef,s=W.directives,a=W.bindings}let d=c?n:new ir(kn(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let ne=(c?f:this.parentInjector).get(ye,null);ne&&(o=ne)}let m=kn(d.componentType??{}),h=jc(this._lContainer,m?.id??null),g=h?.firstChild??null,C=d.create(f,r,g,o,s,a);return this.insertImpl(C.hostView,l,jr(this._hostTNode,h)),C}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,i){let r=n._lView;if(Hg(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Be],l=new e(c,c[st],c[Be]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return ds(s,r,o,i),n.attachToViewContainerRef(),Lu(Df(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=Vv(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),i=ns(this._lContainer,t);i&&(zo(Df(this._lContainer),t),rl(i[A],i))}detach(n){let t=this._adjustIndex(n,-1),i=ns(this._lContainer,t);return i&&zo(Df(this._lContainer),t)!=null?new ai(i):null}_adjustIndex(n,t=0){return n??this.length+t}};function Vv(e){return e[Go]}function Df(e){return e[Go]||(e[Go]=[])}function by(e,n){let t,i=n[e.index];return $t(i)?t=i:(t=sy(i,n,null,e),n[e.index]=t,xm(n,t)),TI(t,n,e,i),new Kf(t,e,n)}function kI(e,n){let t=e[Ce],i=t.createComment(""),r=Gt(n,e),o=t.parentNode(r);return Vc(t,o,i,t.nextSibling(r),!1),i}var TI=NI,RI=()=>!1;function AI(e,n,t){return RI(e,n,t)}function NI(e,n,t,i){if(e[ii])return;let r;t.type&8?r=It(i):r=kI(n,t),e[ii]=r}var Zf=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},Xf=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let i=n.contentQueries!==null?n.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new e(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)Lm(n,t).matches!==null&&this.queries[t].setDirty()}},zc=class{flags;read;predicate;constructor(n,t,i=null){this.flags=t,this.read=i,typeof n=="string"?this.predicate=VI(n):this.predicate=n}},Jf=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},em=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,t,OI(t,o)),this.matchTNodeWithReadOption(n,t,Ac(t,n,o,!1,!1))}else i===dn?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,Ac(t,n,i,!1,!1))}matchTNodeWithReadOption(n,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===$||r===Rt||r===dn&&t.type&4)this.addMatch(t.index,-2);else{let o=Ac(t,n,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function OI(e,n){let t=e.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===n)return t[i+1]}return null}function FI(e,n){return e.type&11?Hr(e,n):e.type&4?al(e,n):null}function PI(e,n,t,i){return t===-1?FI(n,e):t===-2?LI(e,n,i):Jo(e,e[A],t,n)}function LI(e,n,t){if(t===$)return Hr(n,e);if(t===dn)return al(n,e);if(t===Rt)return by(n,e)}function wy(e,n,t,i){let r=n[on].queries[i];if(r.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(PI(n,d,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function tm(e,n,t,i){let r=e.queries.getByIndex(t),o=r.matches;if(o!==null){let s=wy(e,n,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=Ie;f<d.length;f++){let m=d[f];m[ti]===m[Be]&&tm(m[A],m,l,i)}if(d[Qi]!==null){let f=d[Qi];for(let m=0;m<f.length;m++){let h=f[m];tm(h[A],h,l,i)}}}}}return i}function Pm(e,n){return e[on].queries[n].queryList}function Cy(e,n,t){let i=new si((t&4)===4);return $g(e,n,i,i.destroy),(n[on]??=new Xf).queries.push(new Zf(i))-1}function Dy(e,n,t){let i=Ae();return i.firstCreatePass&&(Ey(i,new zc(e,n,t),-1),(n&2)===2&&(i.staticViewQueries=!0)),Cy(i,K(),n)}function xy(e,n,t,i){let r=Ae();if(r.firstCreatePass){let o=Ke();Ey(r,new zc(n,t,i),o.index),jI(r,e),(t&2)===2&&(r.staticContentQueries=!0)}return Cy(r,K(),t)}function VI(e){return e.split(",").map(n=>n.trim())}function Ey(e,n,t){e.queries===null&&(e.queries=new Jf),e.queries.track(new em(n,t))}function jI(e,n){let t=e.contentQueries||(e.contentQueries=[]),i=t.length?t[t.length-1]:-1;n!==i&&t.push(e.queries.length-1,n)}function Lm(e,n){return e.queries.getByIndex(n)}function Iy(e,n){let t=e[A],i=Lm(t,n);return i.crossesNgTemplate?tm(t,e,n,[]):wy(t,e,i,n)}function Sy(e,n,t){let i,r=Io(()=>{i._dirtyCounter();let o=BI(i,e);if(n&&o===void 0)throw new x(-951,!1);return o});return i=r[Ve],i._dirtyCounter=O(0),i._flatValue=void 0,r}function Vm(e){return Sy(!0,!1,e)}function jm(e){return Sy(!0,!0,e)}function My(e,n){let t=e[Ve];t._lView=K(),t._queryIndex=n,t._queryList=Pm(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function BI(e,n){let t=e._lView,i=e._queryIndex;if(t===void 0||i===void 0||t[L]&4)return n?void 0:Ye;let r=Pm(t,i),o=Iy(t,i);return r.reset(o,d_),n?r.first:r._changesDetected||e._flatValue===void 0?e._flatValue=r.toArray():e._flatValue}var Pn=class{},ll=class{};var $c=class extends Pn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Hc(this);constructor(n,t,i,r=!0){super(),this.ngModuleType=n,this._parent=t;let o=Nu(n);this._bootstrapComponents=N_(o.bootstrap),this._r3Injector=ff(n,t,[{provide:Pn,useValue:this},{provide:us,useValue:this.componentFactoryResolver},...i],Bo(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Gc=class extends ll{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new $c(this.moduleType,n,[])}};var is=class extends Pn{injector;componentFactoryResolver=new Hc(this);instance=null;constructor(n){super();let t=new Ui([...n.providers,{provide:Pn,useValue:this},{provide:us,useValue:this.componentFactoryResolver}],n.parent||kr(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function fs(e,n,t=null){return new is({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var HI=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Bu(!1,t.type),r=i.length>0?fs([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:e,providedIn:"environment",factory:()=>new e(z(ye))})}return e})();function M(e){return ss(()=>{let n=ky(e),t=R(_({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===mm.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(HI).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||Qt.Emulated,styles:e.styles||Ye,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&jn("NgStandalone"),Ty(t);let i=e.dependencies;return t.directiveDefs=jv(i,UI),t.pipeDefs=jv(i,Dg),t.id=GI(t),t})}function UI(e){return kn(e)||Ou(e)}function V(e){return ss(()=>({type:e.type,bootstrap:e.bootstrap||Ye,declarations:e.declarations||Ye,imports:e.imports||Ye,exports:e.exports||Ye,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function zI(e,n){if(e==null)return Jn;let t={};for(let i in e)if(e.hasOwnProperty(i)){let r=e[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=tl.None,c=null),t[o]=[i,a,c],n[o]=s}return t}function $I(e){if(e==null)return Jn;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function G(e){return ss(()=>{let n=ky(e);return Ty(n),n})}function ky(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||Jn,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||Ye,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:zI(e.inputs,n),outputs:$I(e.outputs),debugInfo:null}}function Ty(e){e.features?.forEach(n=>n(e))}function jv(e,n){return e?()=>{let t=typeof e=="function"?e():e,i=[];for(let r of t){let o=n(r);o!==null&&i.push(o)}return i}:null}function GI(e){let n=0,t=typeof e.consts=="function"?"":e.consts,i=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function WI(e){return Object.getPrototypeOf(e.prototype).constructor}function ut(e){let n=WI(e.type),t=!0,i=[e];for(;n;){let r;if(sn(e))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new x(903,!1);r=n.\u0275dir}if(r){if(t){i.push(r);let s=e;s.inputs=xf(e.inputs),s.declaredInputs=xf(e.declaredInputs),s.outputs=xf(e.outputs);let a=r.hostBindings;a&&ZI(e,a);let c=r.viewQuery,l=r.contentQueries;if(c&&QI(e,c),l&&KI(e,l),qI(e,r),Cg(e.outputs,r.outputs),sn(r)&&r.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(e),a===ut&&(t=!1)}}n=Object.getPrototypeOf(n)}YI(i)}function qI(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let i=n.inputs[t];i!==void 0&&(e.inputs[t]=i,e.declaredInputs[t]=n.declaredInputs[t])}}function YI(e){let n=0,t=null;for(let i=e.length-1;i>=0;i--){let r=e[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Lr(r.hostAttrs,t=Lr(t,r.hostAttrs))}}function xf(e){return e===Jn?{}:e===Ye?[]:e}function QI(e,n){let t=e.viewQuery;t?e.viewQuery=(i,r)=>{n(i,r),t(i,r)}:e.viewQuery=n}function KI(e,n){let t=e.contentQueries;t?e.contentQueries=(i,r,o)=>{n(i,r,o),t(i,r,o)}:e.contentQueries=n}function ZI(e,n){let t=e.hostBindings;t?e.hostBindings=(i,r)=>{n(i,r),t(i,r)}:e.hostBindings=n}function Ry(e,n,t,i,r,o,s,a){if(t.firstCreatePass){e.mergedAttrs=Lr(e.mergedAttrs,e.attrs);let d=e.tView=Cm(2,e,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),Nr(e,!1);let c=JI(t,n,e,i);xc()&&km(t,n,c,e),Vr(c,n);let l=sy(c,n,c,e);n[i+ke]=l,xm(n,l),AI(l,e,n)}function XI(e,n,t,i,r,o,s,a,c,l,d){let f=t+ke,m;return n.firstCreatePass?(m=$r(n,f,4,s||null,a||null),tf()&&my(n,e,m,Mt(n.consts,l),Y_),Xv(n,m)):m=n.data[f],Ry(m,e,n,t,i,r,o,c),Wo(m)&&Rm(n,e,m),l!=null&&sl(e,m,d),m}function rs(e,n,t,i,r,o,s,a,c,l,d){let f=t+ke,m;if(n.firstCreatePass){if(m=$r(n,f,4,s||null,a||null),l!=null){let h=Mt(n.consts,l);m.localNames=[];for(let g=0;g<h.length;g+=2)m.localNames.push(h[g],-1)}}else m=n.data[f];return Ry(m,e,n,t,i,r,o,c),l!=null&&sl(e,m,d),m}function mn(e,n,t,i,r,o,s,a){let c=K(),l=Ae(),d=Mt(l.consts,o);return XI(c,l,e,n,t,i,r,d,void 0,s,a),mn}var JI=eS;function eS(e,n,t,i){return Ec(!0),n[Ce].createComment("")}var dl=(()=>{class e{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"platform"})}return e})();function Bn(e){return typeof e=="function"&&e[Ve]!==void 0}var Bm=new y("");function ci(e){return!!e&&typeof e.then=="function"}function Hm(e){return!!e&&typeof e.subscribe=="function"}var Ay=new y("");var Um=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=u(Ay,{optional:!0})??[];injector=u(J);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=Qe(this.injector,r);if(ci(o))t.push(o);else if(Hm(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),ul=new y("");function Ny(){Gd(()=>{let e="";throw new x(600,e)})}function Oy(e){return e.isBoundToModule}var tS=10;var yt=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Wt);afterRenderManager=u(il);zonelessEnabled=u(Qo);rootEffectScheduler=u(Ic);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(oi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ce(t=>!t))}constructor(){u(Vn,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=u(ye);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=J.NULL){return this._injector.get(T).run(()=>{fe(se.BootstrapComponentStart);let s=t instanceof cl;if(!this._injector.get(Um).done){let g="";throw new x(405,g)}let c;s?c=t:c=this._injector.get(us).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=Oy(c)?void 0:this._injector.get(Pn),d=i||c.selector,f=c.create(r,[],d,l),m=f.location.nativeElement,h=f.injector.get(Bm,null);return h?.registerApplication(m),f.onDestroy(()=>{this.detachView(f.hostView),Xo(this.components,f),h?.unregisterApplication(m)}),this._loadComponent(f),fe(se.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){fe(se.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(nl.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw fe(se.ChangeDetectionEnd),new x(101,!1);let t=S(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,S(t),this.afterTick.next(),fe(se.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ne,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<tS;){fe(se.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{fe(se.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!qo(r))continue;let o=i&&!this.zonelessEnabled?0:1;ny(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>qo(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Xo(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(ul,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Xo(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new x(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Xo(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function ge(e,n,t,i){let r=K(),o=Xi();if(un(r,o,n)){let s=Ae(),a=Dc();RE(a,r,e,n,t,i)}return ge}var nm=class{destroy(n){}updateValue(n,t){}swap(n,t){let i=Math.min(n,t),r=Math.max(n,t),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,t){this.attach(t,this.detach(n))}};function Ef(e,n,t,i,r){return e===t&&Object.is(n,i)?1:Object.is(r(e,n),r(t,i))?-1:0}function nS(e,n,t,i){let r,o,s=0,a=e.length-1,c=void 0;if(Array.isArray(n)){S(i);let l=n.length-1;for(S(null);s<=a&&s<=l;){let d=e.at(s),f=n[s],m=Ef(s,d,s,f,t);if(m!==0){m<0&&e.updateValue(s,f),s++;continue}let h=e.at(a),g=n[l],C=Ef(a,h,l,g,t);if(C!==0){C<0&&e.updateValue(a,g),a--,l--;continue}let W=t(s,d),ne=t(a,h),Re=t(s,f);if(Object.is(Re,ne)){let jt=t(l,g);Object.is(jt,W)?(e.swap(s,a),e.updateValue(a,g),l--,a--):e.move(a,s),e.updateValue(s,f),s++;continue}if(r??=new Wc,o??=Hv(e,s,a,t),im(e,r,s,Re))e.updateValue(s,f),s++,a++;else if(o.has(Re))r.set(W,e.detach(s)),a--;else{let jt=e.create(s,n[s]);e.attach(s,jt),s++,a++}}for(;s<=l;)Bv(e,r,t,s,n[s]),s++}else if(n!=null){S(i);let l=n[Symbol.iterator]();S(null);let d=l.next();for(;!d.done&&s<=a;){let f=e.at(s),m=d.value,h=Ef(s,f,s,m,t);if(h!==0)h<0&&e.updateValue(s,m),s++,d=l.next();else{r??=new Wc,o??=Hv(e,s,a,t);let g=t(s,m);if(im(e,r,s,g))e.updateValue(s,m),s++,a++,d=l.next();else if(!o.has(g))e.attach(s,e.create(s,m)),s++,a++,d=l.next();else{let C=t(s,f);r.set(C,e.detach(s)),a--}}}for(;!d.done;)Bv(e,r,t,e.length,d.value),d=l.next()}for(;s<=a;)e.destroy(e.detach(a--));r?.forEach(l=>{e.destroy(l)})}function im(e,n,t,i){return n!==void 0&&n.has(i)?(e.attach(t,n.get(i)),n.delete(i),!0):!1}function Bv(e,n,t,i,r){if(im(e,n,i,t(i,r)))e.updateValue(i,r);else{let o=e.create(i,r);e.attach(i,o)}}function Hv(e,n,t,i){let r=new Set;for(let o=n;o<=t;o++)r.add(i(o,e.at(o)));return r}var Wc=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let t=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(n,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,t){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(n,t)}forEach(n){for(let[t,i]of this.kvMap)if(n(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,t)}}};function me(e,n,t,i,r,o,s,a){jn("NgControlFlow");let c=K(),l=Ae(),d=Mt(l.consts,o);return rs(c,l,e,n,t,i,r,d,256,s,a),zm}function zm(e,n,t,i,r,o,s,a){jn("NgControlFlow");let c=K(),l=Ae(),d=Mt(l.consts,o);return rs(c,l,e,n,t,i,r,d,512,s,a),zm}function he(e,n){jn("NgControlFlow");let t=K(),i=Xi(),r=t[i]!==_t?t[i]:-1,o=r!==-1?qc(t,ke+r):void 0,s=0;if(un(t,i,e)){let a=S(null);try{if(o!==void 0&&cy(o,s),e!==-1){let c=ke+e,l=qc(t,c),d=am(t[A],c),f=dy(l,d,t),m=ls(t,d,n,{dehydratedView:f});ds(l,m,s,jr(d,f))}}finally{S(a)}}else if(o!==void 0){let a=ay(o,s);a!==void 0&&(a[Me]=n)}}var rm=class{lContainer;$implicit;$index;constructor(n,t,i){this.lContainer=n,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Ie}};function Gr(e){return e}var om=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,t,i){this.hasEmptyBlock=n,this.trackByFn=t,this.liveCollection=i}};function Xe(e,n,t,i,r,o,s,a,c,l,d,f,m){jn("NgControlFlow");let h=K(),g=Ae(),C=c!==void 0,W=K(),ne=a?s.bind(W[at][Me]):s,Re=new om(C,ne);W[ke+e]=Re,rs(h,g,e+1,n,t,i,r,Mt(g.consts,o),256),C&&rs(h,g,e+2,c,l,d,f,Mt(g.consts,m),512)}var sm=class extends nm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,t,i){super(),this.lContainer=n,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Ie}at(n){return this.getLView(n)[Me].$implicit}attach(n,t){let i=t[Gi];this.needsIndexUpdate||=n!==this.length,ds(this.lContainer,t,n,jr(this.templateTNode,i)),iS(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,rS(this.lContainer,n),oS(this.lContainer,n)}create(n,t){let i=jc(this.lContainer,this.templateTNode.tView.ssrId);return ls(this.hostLView,this.templateTNode,new rm(this.lContainer,t,n),{dehydratedView:i})}destroy(n){rl(n[A],n)}updateValue(n,t){this.getLView(n)[Me].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Me].$index=n}getLView(n){return sS(this.lContainer,n)}};function Je(e){let n=S(null),t=an();try{let i=K(),r=i[A],o=i[t],s=t+1,a=qc(i,s);if(o.liveCollection===void 0){let l=am(r,s);o.liveCollection=new sm(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(nS(c,e,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=Xi(),d=c.length===0;if(un(i,l,d)){let f=t+2,m=qc(i,f);if(d){let h=am(r,f),g=dy(m,h,i),C=ls(i,h,void 0,{dehydratedView:g});ds(m,C,0,jr(h,g))}else r.firstUpdatePass&&tI(m),cy(m,0)}}}finally{S(n)}}function qc(e,n){return e[n]}function iS(e,n){if(e.length<=Ie)return;let t=Ie+n,i=e[t],r=i?i[ni]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Tn];cE(o,r),nr.delete(i[Rn]),r.detachedLeaveAnimationFns=void 0}}function rS(e,n){if(e.length<=Ie)return;let t=Ie+n,i=e[t],r=i?i[ni]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function oS(e,n){return ns(e,n)}function sS(e,n){return ay(e,n)}function am(e,n){return pc(e,n)}function N(e,n,t){let i=K(),r=Xi();if(un(i,r,n)){let o=Ae(),s=Dc();IE(s,i,e,n,i[Ce],t)}return N}function cm(e,n,t,i,r){Am(n,e,t,r?"class":"style",i)}function p(e,n,t,i){let r=K(),o=r[A],s=e+ke,a=o.firstCreatePass?py(s,r,2,n,Y_,tf(),t,i):o.data[s];if(Nn(a)){let c=r[rn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(_y(l),()=>(Uv(e,n,r,a,i),p))}}return Uv(e,n,r,a,i),p}function Uv(e,n,t,i,r){if(Q_(i,t,e,n,Fy),Wo(i)){let o=t[A];Rm(o,t,i),C_(o,i,t)}r!=null&&sl(t,i)}function v(){let e=Ae(),n=Ke(),t=K_(n);return e.firstCreatePass&&gy(e,t),rf(t)&&of(),ef(),t.classesWithoutHost!=null&&ZD(t)&&cm(e,t,K(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&XD(t)&&cm(e,t,K(),t.stylesWithoutHost,!1),v}function le(e,n,t,i){return p(e,n,t,i),v(),le}function At(e,n,t,i){let r=K(),o=r[A],s=e+ke,a=o.firstCreatePass?pI(s,o,2,n,t,i):o.data[s];return Q_(a,r,e,n,Fy),i!=null&&sl(r,a),At}function Nt(){let e=Ke(),n=K_(e);return rf(n)&&of(),ef(),Nt}function hn(e,n,t,i){return At(e,n,t,i),Nt(),hn}var Fy=(e,n,t,i,r)=>(Ec(!0),k_(n[Ce],i,ov()));function Ot(){return K()}function Zt(e,n,t){let i=K(),r=Xi();if(un(i,r,n)){let o=Ae(),s=Dc();q_(s,i,e,n,i[Ce],t)}return Zt}var ms="en-US";var aS=ms;function Py(e){typeof e=="string"&&(aS=e.toLowerCase().replace(/_/g,"-"))}function k(e,n,t){let i=K(),r=Ae(),o=Ke();return cS(r,i,i[Ce],o,e,n,t),k}function cS(e,n,t,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Cf(i,n,o),vI(i,e,n,s,t,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let m=d[f],h=d[f+1];c??=Cf(i,n,o),Pv(i,n,m,h,r,c)}if(l&&l.length)for(let f of l)c??=Cf(i,n,o),Pv(i,n,f,r,r,c)}}function ie(e=1){return rv(e)}function lS(e,n){let t=null,i=qx(e);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){t=r;continue}if(i===null?F_(e,o,!0):Kx(i,o))return r}return t}function De(e){let n=K()[at][st];if(!n.projection){let t=e?e.length:1,i=n.projection=kg(t,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?lS(o,e):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function te(e,n=0,t,i,r,o){let s=K(),a=Ae(),c=i?e+1:null;c!==null&&rs(s,a,c,i,r,o,null,t);let l=$r(a,ke+e,16,null,t||null);l.projection===null&&(l.projection=n),cf();let f=!s[Gi]||nf();s[at][st].projection[l.projection]===null&&c!==null?dS(s,a,c):f&&!Xc(l)&&yE(a,s,l)}function dS(e,n,t){let i=ke+t,r=n.data[i],o=e[i],s=jc(o,r.tView.ssrId),a=ls(e,r,void 0,{dehydratedView:s});ds(o,a,0,jr(r,s))}function Xt(e,n,t,i){return xy(e,n,t,i),Xt}function et(e,n,t){return Dy(e,n,t),et}function re(e){let n=K(),t=Ae(),i=bc();Yo(i+1);let r=Lm(t,i);if(e.dirty&&Bg(n)===((r.metadata.flags&2)===2)){if(r.matches===null)e.reset([]);else{let o=Iy(n,i);e.reset(o,d_),e.notifyOnChanges()}return!0}return!1}function oe(){return Pm(K(),bc())}function fl(e,n,t,i,r){return My(n,xy(e,t,i,r)),fl}function ml(e,n,t,i){return My(e,Dy(n,t,i)),ml}function hl(e=1){Yo(bc()+e)}function li(e){let n=Yg();return jg(n,ke+e)}function kc(e,n){return e<<17|n<<2}function rr(e){return e>>17&32767}function uS(e){return(e&2)==2}function fS(e,n){return e&131071|n<<17}function lm(e){return e|2}function Br(e){return(e&131068)>>2}function If(e,n){return e&-131069|n<<2}function mS(e){return(e&1)===1}function dm(e){return e|1}function hS(e,n,t,i,r,o){let s=o?n.classBindings:n.styleBindings,a=rr(s),c=Br(s);e[i]=t;let l=!1,d;if(Array.isArray(t)){let f=t;d=f[1],(d===null||Mr(f,d)>0)&&(l=!0)}else d=t;if(r)if(c!==0){let m=rr(e[a+1]);e[i+1]=kc(m,a),m!==0&&(e[m+1]=If(e[m+1],i)),e[a+1]=fS(e[a+1],i)}else e[i+1]=kc(a,0),a!==0&&(e[a+1]=If(e[a+1],i)),a=i;else e[i+1]=kc(c,0),a===0?a=i:e[c+1]=If(e[c+1],i),c=i;l&&(e[i+1]=lm(e[i+1])),zv(e,d,i,!0),zv(e,d,i,!1),pS(n,d,e,i,o),s=kc(a,c),o?n.classBindings=s:n.styleBindings=s}function pS(e,n,t,i,r){let o=r?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&Mr(o,n)>=0&&(t[i+1]=dm(t[i+1]))}function zv(e,n,t,i){let r=e[t+1],o=n===null,s=i?rr(r):Br(r),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];gS(c,n)&&(a=!0,e[s+1]=i?dm(l):lm(l)),s=i?rr(l):Br(l)}a&&(e[t+1]=i?lm(r):dm(r))}function gS(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?Mr(e,n)>=0:!1}var Yt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function vS(e){return e.substring(Yt.key,Yt.keyEnd)}function _S(e){return yS(e),Ly(e,Vy(e,0,Yt.textEnd))}function Ly(e,n){let t=Yt.textEnd;return t===n?-1:(n=Yt.keyEnd=bS(e,Yt.key=n,t),Vy(e,n,t))}function yS(e){Yt.key=0,Yt.keyEnd=0,Yt.value=0,Yt.valueEnd=0,Yt.textEnd=e.length}function Vy(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function bS(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function B(e,n){return CS(e,n,null,!0),B}function di(e){DS(kS,wS,e,!0)}function wS(e,n){for(let t=_S(n);t>=0;t=Ly(n,t))fc(e,vS(n),!0)}function CS(e,n,t,i){let r=K(),o=Ae(),s=_c(2);if(o.firstUpdatePass&&By(o,e,s,i),n!==_t&&un(r,s,n)){let a=o.data[an()];Hy(o,a,r,r[Ce],e,r[s+1]=RS(n,t),i,s)}}function DS(e,n,t,i){let r=Ae(),o=_c(2);r.firstUpdatePass&&By(r,null,o,i);let s=K();if(t!==_t&&un(s,o,t)){let a=r.data[an()];if(Uy(a,i)&&!jy(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=ac(c,t||"")),cm(r,a,s,t,i)}else TS(r,a,s,s[Ce],s[o+1],s[o+1]=MS(e,n,t),i,o)}}function jy(e,n){return n>=e.expandoStartIndex}function By(e,n,t,i){let r=e.data;if(r[t+1]===null){let o=r[an()],s=jy(e,t);Uy(o,i)&&n===null&&!s&&(n=!1),n=xS(r,o,n,i),hS(r,o,n,t,s,i)}}function xS(e,n,t,i){let r=ev(e),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(t=Sf(null,e,n,t,i),t=os(t,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==r)if(t=Sf(r,e,n,t,i),o===null){let c=ES(e,n,i);c!==void 0&&Array.isArray(c)&&(c=Sf(null,e,n,c[1],i),c=os(c,n.attrs,i),IS(e,n,i,c))}else o=SS(e,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),t}function ES(e,n,t){let i=t?n.classBindings:n.styleBindings;if(Br(i)!==0)return e[rr(i)]}function IS(e,n,t,i){let r=t?n.classBindings:n.styleBindings;e[rr(r)]=i}function SS(e,n,t){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=e[o].hostAttrs;i=os(i,s,t)}return os(i,n.attrs,t)}function Sf(e,n,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],i=os(i,o.hostAttrs,r),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),i}function os(e,n,t){let i=t?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),fc(e,s,t?!0:n[++o]))}return e===void 0?null:e}function MS(e,n,t){if(t==null||t==="")return Ye;let i=[],r=fn(t);if(Array.isArray(r))for(let o=0;o<r.length;o++)e(i,r[o],!0);else if(r instanceof Set)for(let o of r)e(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&e(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function kS(e,n,t){let i=String(n);i!==""&&!i.includes(" ")&&fc(e,i,t)}function TS(e,n,t,i,r,o,s,a){r===_t&&(r=Ye);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let m=c<r.length?r[c+1]:void 0,h=l<o.length?o[l+1]:void 0,g=null,C;d===f?(c+=2,l+=2,m!==h&&(g=f,C=h)):f===null||d!==null&&d<f?(c+=2,g=d):(l+=2,g=f,C=h),g!==null&&Hy(e,n,t,i,g,C,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function Hy(e,n,t,i,r,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=mS(l)?$v(c,n,t,r,Br(l),s):void 0;if(!Yc(d)){Yc(o)||uS(l)&&(o=$v(c,null,t,r,a,s));let f=Yu(an(),t);wE(i,s,f,r,o)}}function $v(e,n,t,i,r,o){let s=n===null,a;for(;r>0;){let c=e[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,m=t[r+1];m===_t&&(m=f?Ye:void 0);let h=f?mc(m,i):d===i?m:void 0;if(l&&!Yc(h)&&(h=mc(c,i)),Yc(h)&&(a=h,s))return a;let g=e[r+1];r=s?rr(g):Br(g)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=mc(c,i))}return a}function Yc(e){return e!==void 0}function RS(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=Bo(fn(e)))),e}function Uy(e,n){return(e.flags&(n?8:16))!==0}function D(e,n=""){let t=K(),i=Ae(),r=e+ke,o=i.firstCreatePass?$r(i,r,1,n,null):i.data[r],s=AS(i,t,o,n);t[r]=s,xc()&&km(i,t,s,o),Nr(o,!1)}var AS=(e,n,t,i)=>(Ec(!0),Vx(n[Ce],i));function NS(e,n,t,i=""){return un(e,Xi(),t)?n+Uo(t)+i:_t}function OS(e,n,t,i,r,o=""){let s=Qg(),a=gI(e,s,t,r);return _c(2),a?n+Uo(t)+i+Uo(r)+o:_t}function Hn(e){return _e("",e),Hn}function _e(e,n,t){let i=K(),r=NS(i,e,n,t);return r!==_t&&zy(i,an(),r),_e}function pl(e,n,t,i,r){let o=K(),s=OS(o,e,n,t,i,r);return s!==_t&&zy(o,an(),s),pl}function zy(e,n,t){let i=Yu(n,e);jx(e[Ce],i,t)}function Gv(e,n,t){let i=Ae();i.firstCreatePass&&$y(n,i.data,i.blueprint,sn(e),t)}function $y(e,n,t,i,r){if(e=Ge(e),Array.isArray(e))for(let o=0;o<e.length;o++)$y(e[o],n,t,i,r);else{let o=Ae(),s=K(),a=Ke(),c=Hi(e)?e:Ge(e.provide),l=Uu(e),d=a.providerIndexes&1048575,f=a.directiveStart,m=a.providerIndexes>>20;if(Hi(e)||!e.multi){let h=new tr(l,r,dt,null),g=kf(c,n,r?d:d+m,f);g===-1?(Rf(Lc(a,s),o,c),Mf(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(h),s.push(h)):(t[g]=h,s[g]=h)}else{let h=kf(c,n,d+m,f),g=kf(c,n,d,d+m),C=h>=0&&t[h],W=g>=0&&t[g];if(r&&!W||!r&&!C){Rf(Lc(a,s),o,c);let ne=LS(r?PS:FS,t.length,r,i,l,e);!r&&W&&(t[g].providerFactory=ne),Mf(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(ne),s.push(ne)}else{let ne=Gy(t[r?g:h],l,!r&&i);Mf(o,e,h>-1?h:g,ne)}!r&&i&&W&&t[g].componentProviders++}}}function Mf(e,n,t,i){let r=Hi(n),o=Fg(n);if(r||o){let c=(o?Ge(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[i,c]):l[d+1].push(i,c)}else l.push(t,c)}}}function Gy(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function kf(e,n,t,i){for(let r=t;r<i;r++)if(n[r]===e)return r;return-1}function FS(e,n,t,i,r){return um(this.multi,[])}function PS(e,n,t,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Jo(i,i[A],this.providerFactory.index,r);s=c.slice(0,a),um(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],um(o,s);return s}function um(e,n){for(let t=0;t<e.length;t++){let i=e[t];n.push(i())}return n}function LS(e,n,t,i,r,o){let s=new tr(e,t,dt,null);return s.multi=[],s.index=n,s.componentProviders=0,Gy(s,r,i&&!t),s}function Pe(e,n){return t=>{t.providersResolver=(i,r)=>Gv(i,r?r(e):e,!1),n&&(t.viewProvidersResolver=(i,r)=>Gv(i,r?r(n):n,!0))}}function $m(e,n){return al(e,n)}var Qc=class{ngModuleFactory;componentFactories;constructor(n,t){this.ngModuleFactory=n,this.componentFactories=t}},Gm=(()=>{class e{compileModuleSync(t){return new Gc(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=Nu(t),o=N_(r.declarations).reduce((s,a)=>{let c=kn(a);return c&&s.push(new ir(c)),s},[]);return new Qc(i,o)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Wy=(()=>{class e{applicationErrorHandler=u(Wt);appRef=u(yt);taskService=u(oi);ngZone=u(T);zonelessEnabled=u(Qo);tracing=u(Vn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ae;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Vo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(_f,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?lv:mf;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Vo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function qy(){return[{provide:Mn,useExisting:Wy},{provide:T,useClass:jo},{provide:Qo,useValue:!0}]}function VS(){return typeof $localize<"u"&&$localize.locale||ms}var gl=new y("",{factory:()=>u(gl,{optional:!0,skipSelf:!0})||VS()});var vl=class{destroyed=!1;listeners=null;errorHandler=u(xt,{optional:!0});destroyRef=u(Ze);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new x(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let t=this.listeners?.indexOf(n);t!==void 0&&t!==-1&&this.listeners?.splice(t,1)}}}emit(n){if(this.destroyed){console.warn(Ut(953,!1));return}if(this.listeners===null)return;let t=S(null);try{for(let i of this.listeners)try{i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{S(t)}}};function Le(e){return vg(e)}function xe(e,n){return Io(e,n?.equal)}var nb=Symbol("InputSignalNode#UNSET"),iM=R(_({},So),{transformFn:void 0,applyValueToInputSignal(e,n){_r(e,n)}});function ib(e,n){let t=Object.create(iM);t.value=e,t.transformFn=n?.transform;function i(){if(ki(t),t.value===nb){let r=null;throw new x(-950,r)}return t.value}return i[Ve]=t,i}var Un=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Kc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function pn(e){return new vl}function Yy(e,n){return ib(e,n)}function rM(e){return ib(nb,e)}var bt=(Yy.required=rM,Yy);function Qy(e,n){return Vm(n)}function oM(e,n){return jm(n)}var ps=(Qy.required=oM,Qy);function Ky(e,n){return Vm(n)}function sM(e,n){return jm(n)}var rb=(Ky.required=sM,Ky);var qm=new y(""),aM=new y("");function hs(e){return!e.moduleRef}function cM(e){let n=hs(e)?e.r3Injector:e.moduleRef.injector,t=n.get(T);return t.run(()=>{hs(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let i=n.get(Wt),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),hs(e)){let o=()=>n.destroy(),s=e.platformInjector.get(qm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(qm);s.add(o),e.moduleRef.onDestroy(()=>{Xo(e.allPlatformModules,e.moduleRef),r.unsubscribe(),s.delete(o)})}return dM(i,t,()=>{let o=n.get(oi),s=o.add(),a=n.get(Um);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(gl,ms);if(Py(c||ms),!n.get(aM,!0))return hs(e)?n.get(yt):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(hs(e)){let d=n.get(yt);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return lM?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var lM;function dM(e,n,t){try{let i=t();return ci(i)?i.catch(r=>{throw n.runOutsideAngular(()=>e(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>e(i)),i}}var _l=null;function uM(e=[],n){return J.create({name:n,providers:[{provide:$o,useValue:"platform"},{provide:qm,useValue:new Set([()=>_l=null])},...e]})}function fM(e=[]){if(_l)return _l;let n=uM(e);return _l=n,Ny(),mM(n),n}function mM(e){let n=e.get(Zc,null);Qe(e,()=>{n?.forEach(t=>t())})}var hM=1e4;var I3=hM-1e3;var He=(()=>{class e{static __NG_ELEMENT_ID__=pM}return e})();function pM(e){return gM(Ke(),K(),(e&16)===16)}function gM(e,n,t){if(Nn(e)&&!t){let i=St(e.index,n);return new ai(i,i)}else if(e.type&175){let i=n[at];return new ai(i,n)}return null}function ob(e){let{rootComponent:n,appProviders:t,platformProviders:i,platformRef:r}=e;fe(se.BootstrapApplicationStart);try{let o=r?.injector??fM(i),s=[qy(),uv,...t||[]],a=new is({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return cM({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{fe(se.BootstrapApplicationEnd)}}function P(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Ft(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var Wm=Symbol("NOT_SET"),sb=new Set,vM=R(_({},So),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Wm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Wm&&!gr(this))return this.signal;try{for(let r of this.cleanup??sb)r()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=Qn(this),i;try{i=this.userFn.apply(null,n)}finally{Ti(this,t)}return(this.value===Wm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Ym=class extends es{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Ze),s),this.scheduler=r;for(let a of Im){let c=t[a];if(c===void 0)continue;let l=Object.create(vM);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(ki(l),l.value),l.signal[Ve]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??sb)t()}finally{Kn(n)}}};function ab(e,n){let t=n?.injector??u(J),i=t.get(Mn),r=t.get(il),o=t.get(Vn,null,{optional:!0});r.impl??=t.get(Sm);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=t.get(Or,null,{optional:!0}),c=new Ym(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,t,o?.snapshot(null));return r.impl.register(c),c}function yl(e,n){let t=kn(e),i=n.elementInjector||kr();return new ir(t).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var cb=null;function zn(){return cb}function Zm(e){cb??=e}var gs=class{},vs=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(lb),providedIn:"platform"})}return e})();var lb=(()=>{class e extends vs{_location;_history;_doc=u(Y);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return zn().getBaseHref(this._doc)}onPopState(t){let i=zn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=zn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function fb(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function db(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function ui(e){return e&&e[0]!=="?"?`?${e}`:e}var _s=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(mb),providedIn:"root"})}return e})(),_M=new y(""),mb=(()=>{class e extends _s{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(Y).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return fb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+ui(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+ui(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+ui(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||e)(z(vs),z(_M,8))};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var $n=(()=>{class e{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=wM(db(ub(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+ui(i))}normalize(t){return e.stripTrailingSlash(bM(this._basePath,ub(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+ui(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+ui(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ui;static joinWithSlash=fb;static stripTrailingSlash=db;static \u0275fac=function(i){return new(i||e)(z(_s))};static \u0275prov=w({token:e,factory:()=>yM(),providedIn:"root"})}return e})();function yM(){return new $n(z(_s))}function bM(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function ub(e){return e.replace(/\/index.html$/,"")}function wM(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var Xm=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(J);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(t,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||e)(dt(Rt))};static \u0275dir=G({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Oe]})}return e})();function Jm(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var ys=class{};var th="browser";function hb(e){return e===th}var pb=(()=>{class e{static \u0275prov=w({token:e,providedIn:"root",factory:()=>new eh(u(Y),window)})}return e})(),eh=class{document;window;offset=()=>[0,0];constructor(n,t){this.document=n,this.window=t}setOffset(n){Array.isArray(n)?this.offset=()=>n:this.offset=n}getScrollPosition(){return[this.window.scrollX,this.window.scrollY]}scrollToPosition(n,t){this.window.scrollTo(R(_({},t),{left:n[0],top:n[1]}))}scrollToAnchor(n,t){let i=SM(this.document,n);i&&(this.scrollToElement(i,t),i.focus({preventScroll:!0}))}setHistoryScrollRestoration(n){try{this.window.history.scrollRestoration=n}catch{console.warn(Ut(2400,!1))}}scrollToElement(n,t){let i=n.getBoundingClientRect(),r=i.left+this.window.pageXOffset,o=i.top+this.window.pageYOffset,s=this.offset();this.window.scrollTo(R(_({},t),{left:r-s[0],top:o-s[1]}))}};function SM(e,n){let t=e.getElementById(n)||e.getElementsByName(n)[0];if(t)return t;if(typeof e.createTreeWalker=="function"&&e.body&&typeof e.body.attachShadow=="function"){let i=e.createTreeWalker(e.body,NodeFilter.SHOW_ELEMENT),r=i.currentNode;for(;r;){let o=r.shadowRoot;if(o){let s=o.getElementById(n)||o.querySelector(`[name="${n}"]`);if(s)return s}r=i.nextNode()}}return null}var bs=class{_doc;constructor(n){this._doc=n}manager},bl=(()=>{class e extends bs{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||e)(z(Y))};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})(),Dl=new y(""),oh=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof bl));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof bl);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new x(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||e)(z(Dl),z(T))};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})(),nh="ng-app-id";function gb(e){for(let n of e)n.remove()}function vb(e,n){let t=n.createElement("style");return t.textContent=e,t}function MM(e,n,t,i){let r=e.head?.querySelectorAll(`style[${nh}="${n}"],link[${nh}="${n}"]`);if(r)for(let o of r)o.removeAttribute(nh),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function rh(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var sh=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,MM(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,vb);i?.forEach(r=>this.addUsage(r,this.external,rh))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(gb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])gb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,vb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,rh(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||e)(z(Y),z(Ur),z(zr,8),z(or))};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})(),ih={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},ah=/%COMP%/g;var yb="%COMP%",kM=`_nghost-${yb}`,TM=`_ngcontent-${yb}`,RM=!0,AM=new y("",{factory:()=>RM});function NM(e){return TM.replace(ah,e)}function OM(e){return kM.replace(ah,e)}function bb(e,n){return n.map(t=>t.replace(ah,e))}var ch=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new ws(t,s,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Cl?r.applyToHost(t):r instanceof Cs&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Qt.Emulated:o=new Cl(c,l,i,this.appId,d,s,a,f);break;case Qt.ShadowDom:return new wl(c,t,i,s,a,this.nonce,f,l);case Qt.ExperimentalIsolatedShadowDom:return new wl(c,t,i,s,a,this.nonce,f);default:o=new Cs(c,l,i,d,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||e)(z(oh),z(sh),z(Ur),z(AM),z(Y),z(T),z(zr),z(Vn,8))};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})(),ws=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,i,r){this.eventManager=n,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(ih[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(_b(n)?n.content:n).appendChild(t)}insertBefore(n,t,i){n&&(_b(n)?n.content:n).insertBefore(t,i)}removeChild(n,t){t.remove()}selectRootElement(n,t){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new x(-5104,!1);return t||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,i,r){if(r){t=r+":"+t;let o=ih[r];o?n.setAttributeNS(o,t,i):n.setAttribute(t,i)}else n.setAttribute(t,i)}removeAttribute(n,t,i){if(i){let r=ih[i];r?n.removeAttributeNS(r,t):n.removeAttribute(`${i}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,i,r){r&(ln.DashCase|ln.Important)?n.style.setProperty(t,i,r&ln.Important?"important":""):n.style[t]=i}removeStyle(n,t,i){i&ln.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,i){n!=null&&(n[t]=i)}setValue(n,t){n.nodeValue=t}listen(n,t,i,r){if(typeof n=="string"&&(n=zn().getGlobalEventTarget(this.doc,n),!n))throw new x(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,r)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function _b(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var wl=class extends ws{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=bb(i.id,l);for(let f of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=f,this.shadowRoot.appendChild(m)}let d=i.getExternalStyles?.();if(d)for(let f of d){let m=rh(f,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,i){return super.insertBefore(this.nodeOrShadowRoot(n),t,i)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Cs=class extends ws{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?bb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&nr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Cl=class extends Cs{contentAttr;hostAttr;constructor(n,t,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,t,i,o,s,a,c,l),this.contentAttr=NM(l),this.hostAttr=OM(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let i=super.createElement(n,t);return super.setAttribute(i,this.contentAttr,""),i}};var xl=class e extends gs{supportsDOMEvents=!0;static makeCurrent(){Zm(new e)}onAndCancel(n,t,i,r){return n.addEventListener(t,i,r),()=>{n.removeEventListener(t,i,r)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=FM();return t==null?null:PM(t)}resetBaseElement(){Ds=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Jm(document.cookie,n)}},Ds=null;function FM(){return Ds=Ds||document.head.querySelector("base"),Ds?Ds.getAttribute("href"):null}function PM(e){return new URL(e,document.baseURI).pathname}var LM=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})(),wb=["alt","control","meta","shift"],VM={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},jM={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},Cb=(()=>{class e extends bs{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=e.parseEventName(i),a=e.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>zn().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=e._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),wb.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=VM[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),wb.forEach(s=>{if(s!==r){let a=jM[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{e.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||e)(z(Y))};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})();async function lh(e,n,t){let i=_({rootComponent:e},BM(n,t));return ob(i)}function BM(e,n){return{platformRef:n?.platformRef,appProviders:[...GM,...e?.providers??[]],platformProviders:$M}}function HM(){xl.makeCurrent()}function UM(){return new xt}function zM(){return hm(document),document}var $M=[{provide:or,useValue:th},{provide:Zc,useValue:HM,multi:!0},{provide:Y,useFactory:zM}];var GM=[{provide:$o,useValue:"root"},{provide:xt,useFactory:UM},{provide:Dl,useClass:bl,multi:!0},{provide:Dl,useClass:Cb,multi:!0},ch,sh,oh,{provide:Ne,useExisting:ch},{provide:ys,useClass:LM},[]];var Db=(()=>{class e{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||e)(z(Y))};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var dh=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=z(WM),r},providedIn:"root"})}return e})(),WM=(()=>{class e extends dh{_doc;constructor(t){super(),this._doc=t}sanitize(t,i){if(i==null)return null;switch(t){case Kt.NONE:return i;case Kt.HTML:return sr(i,"HTML")?fn(i):bm(this._doc,String(i)).toString();case Kt.STYLE:return sr(i,"Style")?fn(i):i;case Kt.SCRIPT:if(sr(i,"Script"))return fn(i);throw new x(5200,!1);case Kt.URL:return sr(i,"URL")?fn(i):el(String(i));case Kt.RESOURCE_URL:if(sr(i,"ResourceURL"))return fn(i);throw new x(5201,!1);default:throw new x(5202,!1)}}bypassSecurityTrustHtml(t){return pm(t)}bypassSecurityTrustStyle(t){return gm(t)}bypassSecurityTrustScript(t){return vm(t)}bypassSecurityTrustUrl(t){return _m(t)}bypassSecurityTrustResourceUrl(t){return ym(t)}static \u0275fac=function(i){return new(i||e)(z(Y))};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var H="primary",Fs=Symbol("RouteTitle"),ph=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t[0]:t}return null}getAll(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function lr(e){return new ph(e)}function uh(e,n,t){for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(r[0]===":")t[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function Rb(e,n,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>e.length||t.pathMatch==="full"&&(n.hasChildren()||i.length<e.length))return null;let c={},l=e.slice(0,i.length);return uh(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>e.length||t.pathMatch==="full"&&n.hasChildren()&&t.path!=="**")return null;let a={};return!uh(o,e.slice(0,o.length),a)||!uh(s,e.slice(e.length-s.length),a)?null:{consumed:e,posParams:a}}function Tl(e){return new Promise((n,t)=>{e.pipe(En()).subscribe({next:i=>n(i),error:i=>t(i)})})}function YM(e,n){if(e.length!==n.length)return!1;for(let t=0;t<e.length;++t)if(!gn(e[t],n[t]))return!1;return!0}function gn(e,n){let t=e?gh(e):void 0,i=n?gh(n):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let o=0;o<t.length;o++)if(r=t[o],!Ab(e[r],n[r]))return!1;return!0}function gh(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function Ab(e,n){if(Array.isArray(e)&&Array.isArray(n)){if(e.length!==n.length)return!1;let t=[...e].sort(),i=[...n].sort();return t.every((r,o)=>i[o]===r)}else return e===n}function QM(e){return e.length>0?e[e.length-1]:null}function ur(e){return Ao(e)?e:ci(e)?be(Promise.resolve(e)):Z(e)}function Nb(e){return Ao(e)?Tl(e):Promise.resolve(e)}var KM={exact:Pb,subset:Lb},Ob={exact:ZM,subset:XM,ignored:()=>!0},Fb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},vh={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function xb(e,n,t){return KM[t.paths](e.root,n.root,t.matrixParams)&&Ob[t.queryParams](e.queryParams,n.queryParams)&&!(t.fragment==="exact"&&e.fragment!==n.fragment)}function ZM(e,n){return gn(e,n)}function Pb(e,n,t){if(!cr(e.segments,n.segments)||!Sl(e.segments,n.segments,t)||e.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!e.children[i]||!Pb(e.children[i],n.children[i],t))return!1;return!0}function XM(e,n){return Object.keys(n).length<=Object.keys(e).length&&Object.keys(n).every(t=>Ab(e[t],n[t]))}function Lb(e,n,t){return Vb(e,n,n.segments,t)}function Vb(e,n,t,i){if(e.segments.length>t.length){let r=e.segments.slice(0,t.length);return!(!cr(r,t)||n.hasChildren()||!Sl(r,t,i))}else if(e.segments.length===t.length){if(!cr(e.segments,t)||!Sl(e.segments,t,i))return!1;for(let r in n.children)if(!e.children[r]||!Lb(e.children[r],n.children[r],i))return!1;return!0}else{let r=t.slice(0,e.segments.length),o=t.slice(e.segments.length);return!cr(e.segments,r)||!Sl(e.segments,r,i)||!e.children[H]?!1:Vb(e.children[H],n,o,i)}}function Sl(e,n,t){return n.every((i,r)=>Ob[t](e[r].parameters,i.parameters))}var Lt=class{root;queryParams;fragment;_queryParamMap;constructor(n=new de([],{}),t={},i=null){this.root=n,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=lr(this.queryParams),this._queryParamMap}toString(){return tk.serialize(this)}},de=class{segments;children;parent=null;constructor(n,t){this.segments=n,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Ml(this)}},fi=class{path;parameters;_parameterMap;constructor(n,t){this.path=n,this.parameters=t}get parameterMap(){return this._parameterMap??=lr(this.parameters),this._parameterMap}toString(){return Bb(this)}};function JM(e,n){return cr(e,n)&&e.every((t,i)=>gn(t.parameters,n[i].parameters))}function cr(e,n){return e.length!==n.length?!1:e.every((t,i)=>t.path===n[i].path)}function ek(e,n){let t=[];return Object.entries(e.children).forEach(([i,r])=>{i===H&&(t=t.concat(n(r,i)))}),Object.entries(e.children).forEach(([i,r])=>{i!==H&&(t=t.concat(n(r,i)))}),t}var ro=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>new mi,providedIn:"root"})}return e})(),mi=class{parse(n){let t=new yh(n);return new Lt(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(n){let t=`/${xs(n.root,!0)}`,i=rk(n.queryParams),r=typeof n.fragment=="string"?`#${nk(n.fragment)}`:"";return`${t}${i}${r}`}},tk=new mi;function Ml(e){return e.segments.map(n=>Bb(n)).join("/")}function xs(e,n){if(!e.hasChildren())return Ml(e);if(n){let t=e.children[H]?xs(e.children[H],!1):"",i=[];return Object.entries(e.children).forEach(([r,o])=>{r!==H&&i.push(`${r}:${xs(o,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=ek(e,(i,r)=>r===H?[xs(e.children[H],!1)]:[`${r}:${xs(i,!1)}`]);return Object.keys(e.children).length===1&&e.children[H]!=null?`${Ml(e)}/${t[0]}`:`${Ml(e)}/(${t.join("//")})`}}function jb(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function El(e){return jb(e).replace(/%3B/gi,";")}function nk(e){return encodeURI(e)}function _h(e){return jb(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function kl(e){return decodeURIComponent(e)}function Eb(e){return kl(e.replace(/\+/g,"%20"))}function Bb(e){return`${_h(e.path)}${ik(e.parameters)}`}function ik(e){return Object.entries(e).map(([n,t])=>`;${_h(n)}=${_h(t)}`).join("")}function rk(e){let n=Object.entries(e).map(([t,i])=>Array.isArray(i)?i.map(r=>`${El(t)}=${El(r)}`).join("&"):`${El(t)}=${El(i)}`).filter(t=>t);return n.length?`?${n.join("&")}`:""}var ok=/^[^\/()?;#]+/;function fh(e){let n=e.match(ok);return n?n[0]:""}var sk=/^[^\/()?;=#]+/;function ak(e){let n=e.match(sk);return n?n[0]:""}var ck=/^[^=?&#]+/;function lk(e){let n=e.match(ck);return n?n[0]:""}var dk=/^[^&#]+/;function uk(e){let n=e.match(dk);return n?n[0]:""}var yh=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new de([],{}):new de([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(t.length>0||Object.keys(i).length>0)&&(r[H]=new de(t,i)),r}parseSegment(){let n=fh(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(n),new fi(kl(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let t=ak(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=fh(this.remaining);r&&(i=r,this.capture(i))}n[kl(t)]=kl(i)}parseQueryParam(n){let t=lk(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let s=uk(this.remaining);s&&(i=s,this.capture(i))}let r=Eb(t),o=Eb(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=fh(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new x(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=H);let a=this.parseChildren(t+1);i[s??H]=Object.keys(a).length===1&&a[H]?a[H]:new de([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new x(4011,!1)}};function Hb(e){return e.segments.length>0?new de([],{[H]:e}):e}function Ub(e){let n={};for(let[i,r]of Object.entries(e.children)){let o=Ub(r);if(i===H&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let t=new de(e.segments,n);return fk(t)}function fk(e){if(e.numberOfChildren===1&&e.children[H]){let n=e.children[H];return new de(e.segments.concat(n.segments),n.children)}return e}function Kr(e){return e instanceof Lt}function zb(e,n,t=null,i=null,r=new mi){let o=$b(e);return Gb(o,n,t,i,r)}function $b(e){let n;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new de(o.url,s);return o===e&&(n=a),a}let i=t(e.root),r=Hb(i);return n??r}function Gb(e,n,t,i,r){let o=e;for(;o.parent;)o=o.parent;if(n.length===0)return mh(o,o,o,t,i,r);let s=mk(n);if(s.toRoot())return mh(o,o,new de([],{}),t,i,r);let a=hk(s,o,e),c=a.processChildren?Is(a.segmentGroup,a.index,s.commands):qb(a.segmentGroup,a.index,s.commands);return mh(o,a.segmentGroup,c,t,i,r)}function Rl(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function Ms(e){return typeof e=="object"&&e!=null&&e.outlets}function Ib(e,n,t){e||="\u0275";let i=new Lt;return i.queryParams={[e]:n},t.parse(t.serialize(i)).queryParams[e]}function mh(e,n,t,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>Ib(l,f,o)):Ib(l,d,o);let a;e===n?a=t:a=Wb(e,n,t);let c=Hb(Ub(a));return new Lt(c,s,r)}function Wb(e,n,t){let i={};return Object.entries(e.children).forEach(([r,o])=>{o===n?i[r]=t:i[r]=Wb(o,n,t)}),new de(e.segments,i)}var Al=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,t,i){if(this.isAbsolute=n,this.numberOfDoubleDots=t,this.commands=i,n&&i.length>0&&Rl(i[0]))throw new x(4003,!1);let r=i.find(Ms);if(r&&r!==QM(i))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function mk(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new Al(!0,0,e);let n=0,t=!1,i=e.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Al(t,n,i)}var qr=class{segmentGroup;processChildren;index;constructor(n,t,i){this.segmentGroup=n,this.processChildren=t,this.index=i}};function hk(e,n,t){if(e.isAbsolute)return new qr(n,!0,0);if(!t)return new qr(n,!1,NaN);if(t.parent===null)return new qr(t,!0,0);let i=Rl(e.commands[0])?0:1,r=t.segments.length-1+i;return pk(t,r,e.numberOfDoubleDots)}function pk(e,n,t){let i=e,r=n,o=t;for(;o>r;){if(o-=r,i=i.parent,!i)throw new x(4005,!1);r=i.segments.length}return new qr(i,!1,r-o)}function gk(e){return Ms(e[0])?e[0].outlets:{[H]:e}}function qb(e,n,t){if(e??=new de([],{}),e.segments.length===0&&e.hasChildren())return Is(e,n,t);let i=vk(e,n,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<e.segments.length){let o=new de(e.segments.slice(0,i.pathIndex),{});return o.children[H]=new de(e.segments.slice(i.pathIndex),e.children),Is(o,0,r)}else return i.match&&r.length===0?new de(e.segments,{}):i.match&&!e.hasChildren()?bh(e,n,t):i.match?Is(e,0,r):bh(e,n,t)}function Is(e,n,t){if(t.length===0)return new de(e.segments,{});{let i=gk(t),r={};if(Object.keys(i).some(o=>o!==H)&&e.children[H]&&e.numberOfChildren===1&&e.children[H].segments.length===0){let o=Is(e.children[H],n,t);return new de(e.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=qb(e.children[o],n,s))}),Object.entries(e.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new de(e.segments,r)}}function vk(e,n,t){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<e.segments.length;){if(i>=t.length)return o;let s=e.segments[r],a=t[i];if(Ms(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!Mb(c,l,s))return o;i+=2}else{if(!Mb(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function bh(e,n,t){let i=e.segments.slice(0,n),r=0;for(;r<t.length;){let o=t[r];if(Ms(o)){let c=_k(o.outlets);return new de(i,c)}if(r===0&&Rl(t[0])){let c=e.segments[n];i.push(new fi(c.path,Sb(t[0]))),r++;continue}let s=Ms(o)?o.outlets[H]:`${o}`,a=r<t.length-1?t[r+1]:null;s&&a&&Rl(a)?(i.push(new fi(s,Sb(a))),r+=2):(i.push(new fi(s,{})),r++)}return new de(i,{})}function _k(e){let n={};return Object.entries(e).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[t]=bh(new de([],{}),0,i))}),n}function Sb(e){let n={};return Object.entries(e).forEach(([t,i])=>n[t]=`${i}`),n}function Mb(e,n,t){return e==t.path&&gn(n,t.parameters)}var Yr="imperative",Ue=(function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e})(Ue||{}),Ct=class{id;url;constructor(n,t){this.id=n,this.url=t}},hi=class extends Ct{type=Ue.NavigationStart;navigationTrigger;restoredState;constructor(n,t,i="imperative",r=null){super(n,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},vn=class extends Ct{urlAfterRedirects;type=Ue.NavigationEnd;constructor(n,t,i){super(n,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},tt=(function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e[e.Aborted=4]="Aborted",e})(tt||{}),Zr=(function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e})(Zr||{}),Pt=class extends Ct{reason;code;type=Ue.NavigationCancel;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function Yb(e){return e instanceof Pt&&(e.code===tt.Redirect||e.code===tt.SupersededByNewNavigation)}var _n=class extends Ct{reason;code;type=Ue.NavigationSkipped;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}},dr=class extends Ct{error;target;type=Ue.NavigationError;constructor(n,t,i,r){super(n,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},ks=class extends Ct{urlAfterRedirects;state;type=Ue.RoutesRecognized;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Nl=class extends Ct{urlAfterRedirects;state;type=Ue.GuardsCheckStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ol=class extends Ct{urlAfterRedirects;state;shouldActivate;type=Ue.GuardsCheckEnd;constructor(n,t,i,r,o){super(n,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Fl=class extends Ct{urlAfterRedirects;state;type=Ue.ResolveStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Pl=class extends Ct{urlAfterRedirects;state;type=Ue.ResolveEnd;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ll=class{route;type=Ue.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Vl=class{route;type=Ue.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},jl=class{snapshot;type=Ue.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Bl=class{snapshot;type=Ue.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Hl=class{snapshot;type=Ue.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Ul=class{snapshot;type=Ue.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Xr=class{routerEvent;position;anchor;scrollBehavior;type=Ue.Scroll;constructor(n,t,i,r){this.routerEvent=n,this.position=t,this.anchor=i,this.scrollBehavior=r}toString(){let n=this.position?`${this.position[0]}, ${this.position[1]}`:null;return`Scroll(anchor: '${this.anchor}', position: '${n}')`}},Jr=class{},Ts=class{},eo=class{url;navigationBehaviorOptions;constructor(n,t){this.url=n,this.navigationBehaviorOptions=t}};function yk(e){return!(e instanceof Jr)&&!(e instanceof eo)&&!(e instanceof Ts)}var zl=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new oo(this.rootInjector)}},oo=(()=>{class e{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new zl(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||e)(z(ye))};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),$l=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let t=this.pathFromRoot(n);return t.length>1?t[t.length-2]:null}children(n){let t=wh(n,this._root);return t?t.children.map(i=>i.value):[]}firstChild(n){let t=wh(n,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(n){let t=Ch(n,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Ch(n,this._root).map(t=>t.value)}};function wh(e,n){if(e===n.value)return n;for(let t of n.children){let i=wh(e,t);if(i)return i}return null}function Ch(e,n){if(e===n.value)return[n];for(let t of n.children){let i=Ch(e,t);if(i.length)return i.unshift(n),i}return[]}var wt=class{value;children;constructor(n,t){this.value=n,this.children=t}toString(){return`TreeNode(${this.value})`}};function Wr(e){let n={};return e&&e.children.forEach(t=>n[t.value.outlet]=t),n}var Rs=class extends $l{snapshot;constructor(n,t){super(n),this.snapshot=t,Rh(this,n)}toString(){return this.snapshot.toString()}};function Qb(e,n){let t=bk(e,n),i=new je([new fi("",{})]),r=new je({}),o=new je({}),s=new je({}),a=new je(""),c=new pi(i,r,s,a,o,H,e,t.root);return c.snapshot=t.root,new Rs(new wt(c,[]),t)}function bk(e,n){let t={},i={},r={},s=new to([],t,r,"",i,H,e,null,{},n);return new As("",new wt(s,[]))}var pi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,t,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ce(l=>l[Fs]))??Z(void 0),this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ce(n=>lr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ce(n=>lr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Th(e,n,t="emptyOnly"){let i,{routeConfig:r}=e;return n!==null&&(t==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:_(_({},n.params),e.params),data:_(_({},n.data),e.data),resolve:_(_(_(_({},e.data),n.data),r?.data),e._resolvedData)}:i={params:_({},e.params),data:_({},e.data),resolve:_(_({},e.data),e._resolvedData??{})},r&&Zb(r)&&(i.resolve[Fs]=r.title),i}var to=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Fs]}constructor(n,t,i,r,o,s,a,c,l,d){this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=lr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=lr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${t}')`}},As=class extends $l{url;constructor(n,t){super(t),this.url=n,Rh(this,t)}toString(){return Kb(this._root)}};function Rh(e,n){n.value._routerState=e,n.children.forEach(t=>Rh(e,t))}function Kb(e){let n=e.children.length>0?` { ${e.children.map(Kb).join(", ")} } `:"";return`${e.value}${n}`}function hh(e){if(e.snapshot){let n=e.snapshot,t=e._futureSnapshot;e.snapshot=t,gn(n.queryParams,t.queryParams)||e.queryParamsSubject.next(t.queryParams),n.fragment!==t.fragment&&e.fragmentSubject.next(t.fragment),gn(n.params,t.params)||e.paramsSubject.next(t.params),YM(n.url,t.url)||e.urlSubject.next(t.url),gn(n.data,t.data)||e.dataSubject.next(t.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function Dh(e,n){let t=gn(e.params,n.params)&&JM(e.url,n.url),i=!e.parent!=!n.parent;return t&&!i&&(!e.parent||Dh(e.parent,n.parent))}function Zb(e){return typeof e.title=="string"||e.title===null}var Xb=new y(""),Ps=(()=>{class e{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=H;activateEvents=new j;deactivateEvents=new j;attachEvents=new j;detachEvents=new j;routerOutletData=bt();parentContexts=u(oo);location=u(Rt);changeDetector=u(He);inputBinder=u(Yl,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=t;let r=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new xh(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Oe]})}return e})(),xh=class{route;childContexts;parent;outletData;constructor(n,t,i,r){this.route=n,this.childContexts=t,this.parent=i,this.outletData=r}get(n,t){return n===pi?this.route:n===oo?this.childContexts:n===Xb?this.outletData:this.parent.get(n,t)}},Yl=new y("");var Ah=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&le(0,"router-outlet")},dependencies:[Ps],encapsulation:2})}return e})();function Nh(e){let n=e.children&&e.children.map(Nh),t=n?R(_({},e),{children:n}):_({},e);return!t.component&&!t.loadComponent&&(n||t.loadChildren)&&t.outlet&&t.outlet!==H&&(t.component=Ah),t}function wk(e,n,t){let i=Ns(e,n._root,t?t._root:void 0);return new Rs(i,n)}function Ns(e,n,t){if(t&&e.shouldReuseRoute(n.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=n.value;let r=Ck(e,n,t);return new wt(i,r)}else{if(e.shouldAttach(n.value)){let o=e.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>Ns(e,a)),s}}let i=Dk(n.value),r=n.children.map(o=>Ns(e,o));return new wt(i,r)}}function Ck(e,n,t){return n.children.map(i=>{for(let r of t.children)if(e.shouldReuseRoute(i.value,r.value.snapshot))return Ns(e,i,r);return Ns(e,i)})}function Dk(e){return new pi(new je(e.url),new je(e.params),new je(e.queryParams),new je(e.fragment),new je(e.data),e.outlet,e.component,e)}var no=class{redirectTo;navigationBehaviorOptions;constructor(n,t){this.redirectTo=n,this.navigationBehaviorOptions=t}},Jb="ngNavigationCancelingError";function Gl(e,n){let{redirectTo:t,navigationBehaviorOptions:i}=Kr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=ew(!1,tt.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function ew(e,n){let t=new Error(`NavigationCancelingError: ${e||""}`);return t[Jb]=!0,t.cancellationCode=n,t}function xk(e){return tw(e)&&Kr(e.url)}function tw(e){return!!e&&e[Jb]}var Eh=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,t,i,r,o){this.routeReuseStrategy=n,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,n),hh(this.futureState.root),this.activateChildRoutes(t,i,n)}deactivateChildRoutes(n,t,i){let r=Wr(t);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,t,s.children)}else this.deactivateChildRoutes(n,t,i);else o&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(n,t){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,t):this.deactivateRouteAndOutlet(n,t)}detachAndStoreRouteSubtree(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=Wr(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=Wr(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,t,i){let r=Wr(t);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Ul(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Bl(n.value.snapshot))}activateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(hh(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,t,s.children)}else this.activateChildRoutes(n,t,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),hh(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Wl=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Qr=class{component;route;constructor(n,t){this.component=n,this.route=t}};function Ek(e,n,t){let i=e._root,r=n?n._root:null;return Es(i,r,t,[i.value])}function Ik(e){let n=e.routeConfig?e.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:e,guards:n}}function so(e,n){let t=Symbol(),i=n.get(e,t);return i===t?typeof e=="function"&&!Su(e)?e:n.get(e):i}function Es(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Wr(n);return e.children.forEach(s=>{Sk(s,o[s.value.outlet],t,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Ss(a,t.getContext(s),r)),r}function Sk(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=e.value,s=n?n.value:null,a=t?t.getContext(e.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=Mk(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Wl(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Es(e,n,a?a.children:null,i,r):Es(e,n,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Qr(a.outlet.component,s))}else s&&Ss(n,a,r),r.canActivateChecks.push(new Wl(i)),o.component?Es(e,null,a?a.children:null,i,r):Es(e,null,t,i,r);return r}function Mk(e,n,t){if(typeof t=="function")return Qe(n._environmentInjector,()=>t(e,n));switch(t){case"pathParamsChange":return!cr(e.url,n.url);case"pathParamsOrQueryParamsChange":return!cr(e.url,n.url)||!gn(e.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Dh(e,n)||!gn(e.queryParams,n.queryParams);default:return!Dh(e,n)}}function Ss(e,n,t){let i=Wr(e),r=e.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Ss(s,n.children.getContext(o),t):Ss(s,null,t):Ss(s,n,t)}),r.component?n&&n.outlet&&n.outlet.isActivated?t.canDeactivateChecks.push(new Qr(n.outlet.component,r)):t.canDeactivateChecks.push(new Qr(null,r)):t.canDeactivateChecks.push(new Qr(null,r))}function Ls(e){return typeof e=="function"}function kk(e){return typeof e=="boolean"}function Tk(e){return e&&Ls(e.canLoad)}function Rk(e){return e&&Ls(e.canActivate)}function Ak(e){return e&&Ls(e.canActivateChild)}function Nk(e){return e&&Ls(e.canDeactivate)}function Ok(e){return e&&Ls(e.canMatch)}function nw(e){return e instanceof Fi||e?.name==="EmptyError"}var Il=Symbol("INITIAL_VALUE");function io(){return ot(e=>su(e.map(n=>n.pipe(Ht(1),In(Il)))).pipe(ce(n=>{for(let t of n)if(t!==!0){if(t===Il)return Il;if(t===!1||Fk(t))return t}return!0}),we(n=>n!==Il),Ht(1)))}function Fk(e){return Kr(e)||e instanceof no}function iw(e){return e.aborted?Z(void 0).pipe(Ht(1)):new ee(n=>{let t=()=>{n.next(),n.complete()};return e.addEventListener("abort",t),()=>e.removeEventListener("abort",t)})}function rw(e){return $e(iw(e))}function Pk(e){return rt(n=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Z(R(_({},n),{guardsResult:!0})):Lk(o,t,i).pipe(rt(s=>s&&kk(s)?Vk(t,r,e):Z(s)),ce(s=>R(_({},n),{guardsResult:s})))})}function Lk(e,n,t){return be(e).pipe(rt(i=>zk(i.component,i.route,t,n)),En(i=>i!==!0,!0))}function Vk(e,n,t){return be(n).pipe(Ya(i=>xr(Bk(i.route.parent,t),jk(i.route,t),Uk(e,i.path),Hk(e,i.route))),En(i=>i!==!0,!0))}function jk(e,n){return e!==null&&n&&n(new Hl(e)),Z(!0)}function Bk(e,n){return e!==null&&n&&n(new jl(e)),Z(!0)}function Hk(e,n){let t=n.routeConfig?n.routeConfig.canActivate:null;if(!t||t.length===0)return Z(!0);let i=t.map(r=>Pi(()=>{let o=n._environmentInjector,s=so(r,o),a=Rk(s)?s.canActivate(n,e):Qe(o,()=>s(n,e));return ur(a).pipe(En())}));return Z(i).pipe(io())}function Uk(e,n){let t=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>Ik(o)).filter(o=>o!==null).map(o=>Pi(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=so(a,c),d=Ak(l)?l.canActivateChild(t,e):Qe(c,()=>l(t,e));return ur(d).pipe(En())});return Z(s).pipe(io())}));return Z(r).pipe(io())}function zk(e,n,t,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Z(!0);let o=r.map(s=>{let a=n._environmentInjector,c=so(s,a),l=Nk(c)?c.canDeactivate(e,n,t,i):Qe(a,()=>c(e,n,t,i));return ur(l).pipe(En())});return Z(o).pipe(io())}function $k(e,n,t,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Z(!0);let s=o.map(a=>{let c=so(a,e),l=Tk(c)?c.canLoad(n,t):Qe(e,()=>c(n,t)),d=ur(l);return r?d.pipe(rw(r)):d});return Z(s).pipe(io(),ow(i))}function ow(e){return tu(lt(n=>{if(typeof n!="boolean")throw Gl(e,n)}),ce(n=>n===!0))}function Gk(e,n,t,i,r,o){let s=n.canMatch;if(!s||s.length===0)return Z(!0);let a=s.map(c=>{let l=so(c,e),d=Ok(l)?l.canMatch(n,t,r):Qe(e,()=>l(n,t,r));return ur(d).pipe(rw(o))});return Z(a).pipe(io(),ow(i))}var Gn=class e extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,e.prototype)}},Os=class e extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,e.prototype)}};function Wk(e){throw new x(4e3,!1)}function qk(e){throw ew(!1,tt.GuardRejected)}var Ih=class{urlSerializer;urlTree;constructor(n,t){this.urlSerializer=n,this.urlTree=t}async lineralizeSegments(n,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[H])throw Wk(`${n.redirectTo}`);r=r.children[H]}}async applyRedirectCommands(n,t,i,r,o){let s=await Yk(t,r,o);if(s instanceof Lt)throw new Os(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Os(a);return a}applyRedirectCreateUrlTree(n,t,i,r){let o=this.createSegmentGroup(n,t.root,i,r);return new Lt(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(n,t){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=t[a]}else i[r]=o}),i}createSegmentGroup(n,t,i,r){let o=this.createSegments(n,t.segments,i,r),s={};return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new de(o,s)}createSegments(n,t,i,r){return t.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,t,i){let r=i[t.path.substring(1)];if(!r)throw new x(4001,!1);return r}findOrReturn(n,t){let i=0;for(let r of t){if(r.path===n.path)return t.splice(i),r;i++}return n}};function Yk(e,n,t){if(typeof e=="string")return Promise.resolve(e);let i=e;return Tl(ur(Qe(t,()=>i(n))))}function Qk(e,n){return e.providers&&!e._injector&&(e._injector=fs(e.providers,n,`Route: ${e.path}`)),e._injector??n}function Jt(e){return e.outlet||H}function Kk(e,n){let t=e.filter(i=>Jt(i)===n);return t.push(...e.filter(i=>Jt(i)!==n)),t}var Sh={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function sw(e){return{routeConfig:e.routeConfig,url:e.url,params:e.params,queryParams:e.queryParams,fragment:e.fragment,data:e.data,outlet:e.outlet,title:e.title,paramMap:e.paramMap,queryParamMap:e.queryParamMap}}function Zk(e,n,t,i,r,o,s){let a=aw(e,n,t);if(!a.matched)return Z(a);let c=sw(o(a));return i=Qk(n,i),Gk(i,n,t,r,c,s).pipe(ce(l=>l===!0?a:_({},Sh)))}function aw(e,n,t){if(n.path==="")return n.pathMatch==="full"&&(e.hasChildren()||t.length>0)?_({},Sh):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(n.matcher||Rb)(t,e,n);if(!r)return _({},Sh);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?_(_({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function kb(e,n,t,i,r){return t.length>0&&eT(e,t,i,r)?{segmentGroup:new de(n,Jk(i,new de(t,e.children))),slicedSegments:[]}:t.length===0&&tT(e,t,i)?{segmentGroup:new de(e.segments,Xk(e,t,i,e.children)),slicedSegments:t}:{segmentGroup:new de(e.segments,e.children),slicedSegments:t}}function Xk(e,n,t,i){let r={};for(let o of t)if(Ql(e,n,o)&&!i[Jt(o)]){let s=new de([],{});r[Jt(o)]=s}return _(_({},i),r)}function Jk(e,n){let t={};t[H]=n;for(let i of e)if(i.path===""&&Jt(i)!==H){let r=new de([],{});t[Jt(i)]=r}return t}function eT(e,n,t,i){return t.some(r=>!Ql(e,n,r)||!(Jt(r)!==H)?!1:!(i!==void 0&&Jt(r)===i))}function tT(e,n,t){return t.some(i=>Ql(e,n,i))}function Ql(e,n,t){return(e.hasChildren()||n.length>0)&&t.pathMatch==="full"?!1:t.path===""}function nT(e,n,t){return n.length===0&&!e.children[t]}var Mh=class{};async function iT(e,n,t,i,r,o,s="emptyOnly",a){return new kh(e,n,t,i,r,s,o,a).recognize()}var rT=31,kh=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,t,i,r,o,s,a,c){this.injector=n,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Ih(this.urlSerializer,this.urlTree)}noMatchError(n){return new x(4002,`'${n.segmentGroup}'`)}async recognize(){let n=kb(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(n),r=new wt(i,t),o=new As("",r),s=zb(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let t=new to([],Object.freeze({}),Object.freeze(_({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),H,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,H,t),rootSnapshot:t}}catch(i){if(i instanceof Os)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Gn?this.noMatchError(i):i}}async processSegmentGroup(n,t,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,t,i,o);let s=await this.processSegment(n,t,i,i.segments,r,!0,o);return s instanceof wt?[s]:[]}async processChildren(n,t,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=Kk(t,c),f=await this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=cw(s);return oT(a),a}async processSegment(n,t,i,r,o,s,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??n,t,c,i,r,o,s,a)}catch(l){if(l instanceof Gn||nw(l))continue;throw l}if(nT(i,r,o))return new Mh;throw new Gn(i)}async processSegmentAgainstRoute(n,t,i,r,o,s,a,c){if(Jt(i)!==s&&(s===H||!Ql(r,o,i)))throw new Gn(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,t,i,o,s,c);throw new Gn(r)}async expandSegmentAgainstRouteUsingRedirect(n,t,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:m}=aw(t,r,o);if(!c)throw new Gn(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>rT&&(this.allowRedirects=!1));let h=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let g=await this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,sw(h),n),C=await this.applyRedirects.lineralizeSegments(r,g);return this.processSegment(n,i,t,C.concat(m),s,!1,a)}createSnapshot(n,t,i,r,o){let s=new to(i,r,Object.freeze(_({},this.urlTree.queryParams)),this.urlTree.fragment,aT(t),Jt(t),t.component??t._loadedComponent??null,t,cT(t),n),a=Th(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,t,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=jt=>this.createSnapshot(n,i,jt.consumedSegments,jt.parameters,s),c=await Tl(Zk(t,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new Gn(t);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:m,remainingSegments:h}=c,g=this.createSnapshot(n,i,m,f,s),{segmentGroup:C,slicedSegments:W}=kb(t,m,h,l,o);if(W.length===0&&C.hasChildren()){let jt=await this.processChildren(d,l,C,g);return new wt(g,jt)}if(l.length===0&&W.length===0)return new wt(g,[]);let ne=Jt(i)===o,Re=await this.processSegment(d,l,C,W,ne?H:o,!0,g);return new wt(g,Re instanceof wt?[Re]:[])}async getChildConfig(n,t,i){if(t.children)return{routes:t.children,injector:n};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(n).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Tl($k(n,t,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw qk(t)}return{routes:[],injector:n}}};function oT(e){e.sort((n,t)=>n.value.outlet===H?-1:t.value.outlet===H?1:n.value.outlet.localeCompare(t.value.outlet))}function sT(e){let n=e.value.routeConfig;return n&&n.path===""}function cw(e){let n=[],t=new Set;for(let i of e){if(!sT(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):n.push(i)}for(let i of t){let r=cw(i.children);n.push(new wt(i.value,r))}return n.filter(i=>!t.has(i))}function aT(e){return e.data||{}}function cT(e){return e.resolve||{}}function lT(e,n,t,i,r,o,s){return rt(async a=>{let{state:c,tree:l}=await iT(e,n,t,i,a.extractedUrl,r,o,s);return R(_({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function dT(e){return rt(n=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=n;if(!i.length)return Z(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of lw(a))o.add(c);let s=0;return be(o).pipe(Ya(a=>r.has(a)?uT(a,t,e):(a.data=Th(a,a.parent,e).resolve,Z(void 0))),lt(()=>s++),Ka(1),rt(a=>s===o.size?Z(n):Se))})}function lw(e){let n=e.children.map(t=>lw(t)).flat();return[e,...n]}function uT(e,n,t){let i=e.routeConfig,r=e._resolve;return i?.title!==void 0&&!Zb(i)&&(r[Fs]=i.title),Pi(()=>(e.data=Th(e,e.parent,t).resolve,fT(r,e,n).pipe(ce(o=>(e._resolvedData=o,e.data=_(_({},e.data),o),null)))))}function fT(e,n,t){let i=gh(e);if(i.length===0)return Z({});let r={};return be(i).pipe(rt(o=>mT(e[o],n,t).pipe(En(),lt(s=>{if(s instanceof no)throw Gl(new mi,s);r[o]=s}))),Ka(1),ce(()=>r),No(o=>nw(o)?Se:ou(o)))}function mT(e,n,t){let i=n._environmentInjector,r=so(e,i),o=r.resolve?r.resolve(n,t):Qe(i,()=>r(n,t));return ur(o)}function Tb(e){return ot(n=>{let t=e(n);return t?be(t).pipe(ce(()=>n)):Z(n)})}var Oh=(()=>{class e{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===H);return i}getResolvedTitleForRoute(t){return t.data[Fs]}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(dw),providedIn:"root"})}return e})(),dw=(()=>{class e extends Oh{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||e)(z(Db))};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Vs=new y("",{factory:()=>({})}),js=new y(""),uw=(()=>{class e{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(Gm);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await Nb(Qe(t,()=>i.loadComponent())),s=await hw(mw(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await fw(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();async function fw(e,n,t,i){let r=await Nb(Qe(t,()=>e.loadChildren())),o=await hw(mw(r)),s;o instanceof ll||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(e);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,d=s,c=a.get(js,[],{optional:!0,self:!0}).flat()),{routes:c.map(Nh),injector:a,factory:d}}function hT(e){return e&&typeof e=="object"&&"default"in e}function mw(e){return hT(e)?e.default:e}async function hw(e){return e}var Kl=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(pT),providedIn:"root"})}return e})(),pT=(()=>{class e{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),pw=new y("");var gT=()=>{},gw=new y(""),Fh=(()=>{class e{currentNavigation=O(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=O(null);events=new E;transitionAbortWithErrorSubject=new E;configLoader=u(uw);environmentInjector=u(ye);destroyRef=u(Ze);urlSerializer=u(ro);rootContexts=u(oo);location=u($n);inputBindingEnabled=u(Yl,{optional:!0})!==null;titleStrategy=u(Oh);options=u(Vs,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=u(Kl);createViewTransition=u(pw,{optional:!0});navigationErrorHandler=u(gw,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Z(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Ll(r)),i=r=>this.events.next(new Vl(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;Le(()=>{this.transitions?.next(R(_({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new je(null),this.transitions.pipe(we(i=>i!==null),ot(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return Z(i).pipe(ot(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",tt.SupersededByNewNavigation),Se;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?R(_({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),d=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&d!=="reload")return this.events.next(new _n(a.id,this.urlSerializer.serialize(a.rawUrl),"",Zr.IgnoredSameUrlNavigation)),a.resolve(!1),Se;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return Z(a).pipe(ot(f=>(this.events.next(new hi(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?Se:Promise.resolve(f))),lT(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),lt(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(m=>(m.finalUrl=f.urlAfterRedirects,m)),this.events.next(new Ts)}),ot(f=>be(i.routesRecognizeHandler.deferredHandle??Z(void 0)).pipe(ce(()=>f))),lt(()=>{let f=new ks(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:f,extractedUrl:m,source:h,restoredState:g,extras:C}=a,W=new hi(f,this.urlSerializer.serialize(m),h,g);this.events.next(W);let ne=Qb(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=R(_({},a),{targetSnapshot:ne,urlAfterRedirects:m,extras:R(_({},C),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Re=>(Re.finalUrl=m,Re)),Z(i)}else return this.events.next(new _n(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Zr.IgnoredByUrlHandlingStrategy)),a.resolve(!1),Se}),ce(a=>{let c=new Nl(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=R(_({},a),{guards:Ek(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),Pk(a=>this.events.next(a)),ot(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw Gl(this.urlSerializer,a.guardsResult);let c=new Ol(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return Se;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",tt.GuardRejected),Se;if(a.guards.canActivateChecks.length===0)return Z(a);let l=new Fl(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return Se;let d=!1;return Z(a).pipe(dT(this.paramsInheritanceStrategy),lt({next:()=>{d=!0;let f=new Pl(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)},complete:()=>{d||this.cancelNavigationTransition(a,"",tt.NoDataFromResolver)}}))}),Tb(a=>{let c=d=>{let f=[];if(d.routeConfig?._loadedComponent)d.component=d.routeConfig?._loadedComponent;else if(d.routeConfig?.loadComponent){let m=d._environmentInjector;f.push(this.configLoader.loadComponent(m,d.routeConfig).then(h=>{d.component=h}))}for(let m of d.children)f.push(...c(m));return f},l=c(a.targetSnapshot.root);return l.length===0?Z(a):be(Promise.all(l).then(()=>a))}),Tb(()=>this.afterPreactivation()),ot(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?be(l).pipe(ce(()=>i)):Z(i)}),Ht(1),ot(a=>{let c=wk(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=R(_({},a),{targetRouterState:c}),this.currentNavigation.update(d=>(d.targetRouterState=c,d)),this.events.next(new Jr);let l=i.beforeActivateHandler.deferredHandle;return l?be(l.then(()=>a)):Z(a)}),lt(a=>{new Eh(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=gT,c)),this.lastSuccessfulNavigation.set(Le(this.currentNavigation)),this.events.next(new vn(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),$e(iw(o.signal).pipe(we(()=>!r&&!i.targetRouterState),lt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",tt.Aborted)}))),lt({complete:()=>{r=!0}}),$e(this.transitionAbortWithErrorSubject.pipe(lt(a=>{throw a}))),lu(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",tt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),No(a=>{if(r=!0,this.destroyed)return i.resolve(!1),Se;if(tw(a))this.events.next(new Pt(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),xk(a)?this.events.next(new eo(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new dr(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=Qe(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof no){let{message:d,cancellationCode:f}=Gl(this.urlSerializer,l);this.events.next(new Pt(i.id,this.urlSerializer.serialize(i.extractedUrl),d,f)),this.events.next(new eo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return Se}))}))}cancelNavigationTransition(t,i,r){let o=new Pt(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Le(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function vT(e){return e!==Yr}var vw=new y("");var _w=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(_T),providedIn:"root"})}return e})(),ql=class{shouldDetach(n){return!1}store(n,t){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,t){return n.routeConfig===t.routeConfig}shouldDestroyInjector(n){return!0}},_T=(()=>{class e extends ql{static \u0275fac=(()=>{let t;return function(r){return(t||(t=kt(e)))(r||e)}})();static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Ph=(()=>{class e{urlSerializer=u(ro);options=u(Vs,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u($n);urlHandlingStrategy=u(Kl);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Lt;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,s=r??o;return s instanceof Lt?this.urlSerializer.serialize(s):s}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=Qb(null,u(ye));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:()=>u(yT),providedIn:"root"})}return e})(),yT=(()=>{class e extends Ph{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof hi?this.updateStateMemento():t instanceof _n?this.commitTransition(i):t instanceof ks?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Jr?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Pt&&!Yb(t)?this.restoreHistory(i):t instanceof dr?this.restoreHistory(i,!0):t instanceof vn&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||s){let c=this.browserPageId,l=_(_({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(t,"",l)}else{let c=_(_({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?_({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):_({navigationId:t},this.routerUrlState(r))}static \u0275fac=(()=>{let t;return function(r){return(t||(t=kt(e)))(r||e)}})();static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Lh(e,n){e.events.pipe(we(t=>t instanceof vn||t instanceof Pt||t instanceof dr||t instanceof _n),ce(t=>t instanceof vn||t instanceof _n?0:(t instanceof Pt?t.code===tt.Redirect||t.code===tt.SupersededByNewNavigation:!1)?2:1),we(t=>t!==2),Ht(1)).subscribe(()=>{n()})}var yn=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(dl);stateManager=u(Ph);options=u(Vs,{optional:!0})||{};pendingTasks=u(oi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(Fh);urlSerializer=u(ro);location=u($n);urlHandlingStrategy=u(Kl);injector=u(ye);_events=new E;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(_w);injectorCleanup=u(vw,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(js,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Yl,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ae;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Le(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Pt&&i.code!==tt.Redirect&&i.code!==tt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof vn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof eo){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=_({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||vT(r.source)},s);this.scheduleNavigation(a,Yr,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}yk(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Yr,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,o)=>{this.navigateToSyncWithBrowser(t,r,i,o)})}navigateToSyncWithBrowser(t,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(o=R(_({},o),{browserUrl:t})),r){let l=_({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Wt)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Le(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(Nh),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=_(_({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let m=r?r.snapshot:this.routerState.snapshot.root;f=$b(m)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),f=this.currentUrlTree.root}return Gb(f,t,d,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=Kr(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Yr,null,i)}navigate(t,i={skipLocationChange:!1}){return bT(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Ut(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=_({},Fb):i===!1?r=_({},vh):r=_(_({},vh),i),Kr(t))return xb(this.currentUrlTree,t,r);let o=this.parseUrl(t);return xb(this.currentUrlTree,o,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(t,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,m)=>{a=f,c=m});let d=this.pendingTasks.add();return Lh(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function bT(e){for(let n=0;n<e.length;n++)if(e[n]==null)throw new x(4008,!1)}var yw=new y(""),CT=(()=>{class e{options;routerEventsSubscription;scrollEventsSubscription;lastId=0;lastSource=Yr;restoredId=0;store={};urlSerializer=u(ro);zone=u(T);viewportScroller=u(pb);transitions=u(Fh);constructor(t){this.options=t,this.options.scrollPositionRestoration||="disabled",this.options.anchorScrolling||="disabled"}init(){this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.setHistoryScrollRestoration("manual"),this.routerEventsSubscription=this.createScrollEvents(),this.scrollEventsSubscription=this.consumeScrollEvents()}createScrollEvents(){return this.transitions.events.subscribe(t=>{t instanceof hi?(this.store[this.lastId]=this.viewportScroller.getScrollPosition(),this.lastSource=t.navigationTrigger,this.restoredId=t.restoredState?t.restoredState.navigationId:0):t instanceof vn?(this.lastId=t.id,this.scheduleScrollEvent(t,this.urlSerializer.parse(t.urlAfterRedirects).fragment)):t instanceof _n&&t.code===Zr.IgnoredSameUrlNavigation&&(this.lastSource=void 0,this.restoredId=0,this.scheduleScrollEvent(t,this.urlSerializer.parse(t.url).fragment))})}consumeScrollEvents(){return this.transitions.events.subscribe(t=>{if(!(t instanceof Xr)||t.scrollBehavior==="manual")return;let i={behavior:"instant"};t.position?this.options.scrollPositionRestoration==="top"?this.viewportScroller.scrollToPosition([0,0],i):this.options.scrollPositionRestoration==="enabled"&&this.viewportScroller.scrollToPosition(t.position,i):t.anchor&&this.options.anchorScrolling==="enabled"?this.viewportScroller.scrollToAnchor(t.anchor):this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.scrollToPosition([0,0])})}scheduleScrollEvent(t,i){let r=Le(this.transitions.currentNavigation)?.extras.scroll;this.zone.runOutsideAngular(async()=>{await new Promise(o=>{setTimeout(o),typeof requestAnimationFrame<"u"&&requestAnimationFrame(o)}),this.zone.run(()=>{this.transitions.events.next(new Xr(t,this.lastSource==="popstate"?this.store[this.restoredId]:null,i,r))})})}ngOnDestroy(){this.routerEventsSubscription?.unsubscribe(),this.scrollEventsSubscription?.unsubscribe()}static \u0275fac=function(i){Fm()};static \u0275prov=w({token:e,factory:e.\u0275fac})}return e})();function Vh(e,...n){return $i([{provide:js,multi:!0,useValue:e},[],{provide:pi,useFactory:DT},{provide:ul,multi:!0,useFactory:ET},n.map(t=>t.\u0275providers)])}function DT(){return u(yn).routerState.root}function xT(e,n){return{\u0275kind:e,\u0275providers:n}}function jh(e={}){return xT(4,[{provide:yw,useFactory:()=>new CT(e)}])}function ET(){let e=u(J);return n=>{let t=e.get(yt);if(n!==t.components[0])return;let i=e.get(yn),r=e.get(IT);e.get(ST)===1&&i.initialNavigation(),e.get(MT,null,{optional:!0})?.setUpPreloading(),e.get(yw,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var IT=new y("",{factory:()=>new E}),ST=new y("",{factory:()=>1});var MT=new y("");var TT=new y("cdk-dir-doc",{providedIn:"root",factory:()=>u(Y)}),RT=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function bw(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?RT.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Wn=(()=>{class e{get value(){return this.valueSignal()}valueSignal=O("ltr");change=new j;constructor(){let t=u(TT,{optional:!0});if(t){let i=t.body?t.body.dir:null,r=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(bw(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var pe=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({})}return e})();var AT=["*"];var NT=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],OT=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],FT=new y("MAT_CARD_CONFIG"),gi=(()=>{class e{appearance;constructor(){let t=u(FT,{optional:!0});this.appearance=t?.appearance||"raised"}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&B("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:AT,decls:1,vars:0,template:function(i,r){i&1&&(De(),te(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return e})(),vi=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return e})();var _i=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return e})(),ao=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return e})(),ww=(()=>{class e{align="start";static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&B("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return e})(),yi=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:OT,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(De(NT),te(0),At(1,"div",0),te(2,1),Nt(),te(3,2))},encapsulation:2,changeDetection:0})}return e})();var bn=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[pe]})}return e})();function Dt(e){return e instanceof $?e.nativeElement:e}function Hh(e){return Array.isArray(e)?e:[e]}function Te(e){return e==null?"":typeof e=="string"?e:`${e}px`}function qn(e){return e!=null&&`${e}`!="false"}var Uh;try{Uh=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Uh=!1}var Ee=(()=>{class e{_platformId=u(or);isBrowser=this._platformId?hb(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Uh)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var fr;function Dw(){if(fr==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return fr=!1,fr;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)fr=!0;else{let e=Element.prototype.scrollTo;e?fr=!/\{\s*\[native code\]\s*\}/.test(e.toString()):fr=!1}}return fr}var zh;function xw(){if(zh==null){let e=typeof document<"u"?document.head:null;zh=!!(e&&(e.createShadowRoot||e.attachShadow))}return zh}function $h(e){if(xw()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function We(e){return e.composedPath?e.composedPath()[0]:e.target}function Gh(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Bs;function Ew(){if(Bs==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Bs=!0}))}finally{Bs=Bs||!1}return Bs}function co(e){return Ew()?e:!!e.capture}var lo,Iw=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Wh(){if(lo)return lo;if(typeof document!="object"||!document)return lo=new Set(Iw),lo;let e=document.createElement("input");return lo=new Set(Iw.filter(n=>(e.setAttribute("type",n),e.type===n))),lo}var Zl=new WeakMap,nt=(()=>{class e{_appRef;_injector=u(J);_environmentInjector=u(ye);load(t){let i=this._appRef=this._appRef||this._injector.get(yt),r=Zl.get(i);r||(r={loaders:new Set,refs:[]},Zl.set(i,r),i.onDestroy(()=>{Zl.get(i)?.refs.forEach(o=>o.destroy()),Zl.delete(i)})),r.loaders.has(t)||(r.loaders.add(t),r.refs.push(yl(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var PT=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return e})(),LT={passive:!0},Sw=(()=>{class e{_platform=u(Ee);_ngZone=u(T);_renderer=u(Ne).createRenderer(null,null);_styleLoader=u(nt);_monitoredElements=new Map;constructor(){}monitor(t){if(!this._platform.isBrowser)return Se;this._styleLoader.load(PT);let i=Dt(t),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new E,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,LT)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(t){let i=Dt(t),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((t,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Mw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({})}return e})();function Hs(e){return e.buttons===0||e.detail===0}function Us(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var kw=new y("cdk-input-modality-detector-options"),Tw={ignoreKeys:[18,17,224,91,16]},Rw=650,qh={passive:!0,capture:!0},Aw=(()=>{class e{_platform=u(Ee);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new je(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(i=>i===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=We(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<Rw||(this._modality.next(Hs(t)?"keyboard":"mouse"),this._mostRecentTarget=We(t))};_onTouchstart=t=>{if(Us(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=We(t)};constructor(){let t=u(T),i=u(Y),r=u(kw,{optional:!0});if(this._options=_(_({},Tw),r),this.modalityDetected=this._modality.pipe(uu(1)),this.modalityChanged=this.modalityDetected.pipe(Qa()),this._platform.isBrowser){let o=u(Ne).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,qh),o.listen(i,"mousedown",this._onMousedown,qh),o.listen(i,"touchstart",this._onTouchstart,qh)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),zs=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(zs||{}),Nw=new y("cdk-focus-monitor-default-options"),Xl=co({passive:!0,capture:!0}),mr=(()=>{class e{_ngZone=u(T);_platform=u(Ee);_inputModalityDetector=u(Aw);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(Y);_stopInputModalityDetector=new E;constructor(){let t=u(Nw,{optional:!0});this._detectionMode=t?.detectionMode||zs.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let i=We(t);for(let r=i;r;r=r.parentElement)t.type==="focus"?this._onFocus(t,r):this._onBlur(t,r)};monitor(t,i=!1){let r=Dt(t);if(!this._platform.isBrowser||r.nodeType!==1)return Z();let o=$h(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new E,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let i=Dt(t),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(t,i,r){let o=Dt(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((t,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===zs.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,i){t.classList.toggle("cdk-focused",!!i),t.classList.toggle("cdk-touch-focused",i==="touch"),t.classList.toggle("cdk-keyboard-focused",i==="keyboard"),t.classList.toggle("cdk-mouse-focused",i==="mouse"),t.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(t,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&i,this._detectionMode===zs.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?Rw:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(t,i){let r=this._elementInfo.get(i),o=We(t);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(t,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&t.relatedTarget instanceof Node&&i.contains(t.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(t,i){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(i))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let i=t.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Xl),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Xl)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe($e(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let i=t.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Xl),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Xl),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,i,r){this._setClasses(t,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(t){let i=[];return this._elementInfo.forEach((r,o)=>{(o===t||r.checkChildren&&o.contains(t))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Yh=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})(),Jl;function VT(){if(Jl===void 0&&(Jl=null,typeof window<"u")){let e=window;e.trustedTypes!==void 0&&(Jl=e.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Jl}function jT(e){return VT()?.createHTML(e)||e}function Ow(e,n,t){let i=t.sanitize(Kt.HTML,n);e.innerHTML=jT(i||"")}var Fw=new Set,hr,Qh=(()=>{class e{_platform=u(Ee);_nonce=u(zr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):HT}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&BT(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function BT(e,n){if(!Fw.has(e))try{hr||(hr=document.createElement("style"),n&&hr.setAttribute("nonce",n),hr.setAttribute("type","text/css"),document.head.appendChild(hr)),hr.sheet&&(hr.sheet.insertRule(`@media ${e} {body{ }}`,0),Fw.add(e))}catch(t){console.error(t)}}function HT(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var UT=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Pw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({providers:[UT]})}return e})();var Lw=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Vw=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),zT=0,Kh=(()=>{class e{_ngZone=u(T);_defaultOptions=u(Vw,{optional:!0});_liveElement;_document=u(Y);_sanitizer=u(dh);_previousTimeout;_currentPromise;_currentResolve;constructor(){let t=u(Lw,{optional:!0});this._liveElement=t||this._createLiveElement()}announce(t,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!t||typeof t=="string"?this._liveElement.textContent=t:Ow(this._liveElement,t,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let t="cdk-live-announcer-element",i=this._document.getElementsByClassName(t),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(t),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${zT++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(t){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(t)===-1&&o.setAttribute("aria-owns",s+" "+t):o.setAttribute("aria-owns",t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var $T=200,ed=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(n,t){let i=typeof t?.debounceInterval=="number"?t.debounceInterval:$T;t?.skipPredicate&&(this._skipPredicateFn=t.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let t=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(t>=65&&t<=90||t>=48&&t<=57)&&this._letterKeyStream.next(String.fromCharCode(t))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(lt(t=>this._pressedLetters.push(t)),cu(n),we(()=>this._pressedLetters.length>0),ce(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(t=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(t)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function wn(e,...n){return n.length?n.some(t=>e[t]):e.altKey||e.shiftKey||e.ctrlKey||e.metaKey}var td=class{_items;_activeItemIndex=O(-1);_activeItem=O(null);_wrap=!1;_typeaheadSubscription=ae.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,t){this._items=n,n instanceof si?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Bn(n)&&(this._effectRef=On(()=>this._itemsChanged(n()),{injector:t}))}tabOut=new E;change=new E;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let t=this._getItemsArray();return this._typeahead=new ed(t,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,t=10){return this._pageUpAndDown={enabled:n,delta:t},this}setActiveItem(n){let t=this._activeItem();this.updateActiveItem(n),this._activeItem()!==t&&this.change.next(this._activeItemIndex())}onKeydown(n){let t=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(t){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||wn(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let t=this._getItemsArray(),i=typeof n=="number"?n:t.indexOf(n),r=t[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let t=this._getItemsArray();for(let i=1;i<=t.length;i++){let r=(this._activeItemIndex()+n*i+t.length)%t.length,o=t[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,t){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=t,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Bn(this._items)?this._items():this._items instanceof si?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let t=this._activeItem();if(t){let i=n.indexOf(t);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var $s=class extends td{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Jh={},ze=class e{_appId=u(Ur);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){return this._appId!=="ng"&&(n+=this._appId),Jh.hasOwnProperty(n)||(Jh[n]=0),`${n}${t?e._infix+"-":""}${Jh[n]++}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})};var Uw=" ";function zw(e,n,t){let i=$w(e,n);t=t.trim(),!i.some(r=>r.trim()===t)&&(i.push(t),e.setAttribute(n,i.join(Uw)))}function ep(e,n,t){let i=$w(e,n);t=t.trim();let r=i.filter(o=>o!==t);r.length?e.setAttribute(n,r.join(Uw)):e.removeAttribute(n)}function $w(e,n){return e.getAttribute(n)?.match(/\S+/g)??[]}var fd=new y("");function ip(e){return e==null||rp(e)===0}function rp(e){return e==null?null:Array.isArray(e)||typeof e=="string"?e.length:e instanceof Set?e.size:null}var md=new y(""),Zw=new y(""),GT=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,mo=class{static min(n){return WT(n)}static max(n){return qT(n)}static required(n){return YT(n)}static requiredTrue(n){return QT(n)}static email(n){return KT(n)}static minLength(n){return ZT(n)}static maxLength(n){return XT(n)}static pattern(n){return JT(n)}static nullValidator(n){return Xw()}static compose(n){return r0(n)}static composeAsync(n){return o0(n)}};function WT(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t<e?{min:{min:e,actual:n.value}}:null}}function qT(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t>e?{max:{max:e,actual:n.value}}:null}}function YT(e){return ip(e.value)?{required:!0}:null}function QT(e){return e.value===!0?null:{required:!0}}function KT(e){return ip(e.value)||GT.test(e.value)?null:{email:!0}}function ZT(e){return n=>{let t=n.value?.length??rp(n.value);return t===null||t===0?null:t<e?{minlength:{requiredLength:e,actualLength:t}}:null}}function XT(e){return n=>{let t=n.value?.length??rp(n.value);return t!==null&&t>e?{maxlength:{requiredLength:e,actualLength:t}}:null}}function JT(e){if(!e)return Xw;let n,t;return typeof e=="string"?(t="",e.charAt(0)!=="^"&&(t+="^"),t+=e,e.charAt(e.length-1)!=="$"&&(t+="$"),n=new RegExp(t)):(t=e.toString(),n=e),i=>{if(ip(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function Xw(e){return null}function Jw(e){return e!=null}function e0(e){return ci(e)?be(e):e}function t0(e){let n={};return e.forEach(t=>{n=t!=null?_(_({},n),t):n}),Object.keys(n).length===0?null:n}function n0(e,n){return n.map(t=>t(e))}function eR(e){return!e.validate}function i0(e){return e.map(n=>eR(n)?n:t=>n.validate(t))}function r0(e){if(!e)return null;let n=e.filter(Jw);return n.length==0?null:function(t){return t0(n0(t,n))}}function op(e){return e!=null?r0(i0(e)):null}function o0(e){if(!e)return null;let n=e.filter(Jw);return n.length==0?null:function(t){let i=n0(t,n).map(e0);return au(i).pipe(ce(t0))}}function sp(e){return e!=null?o0(i0(e)):null}function Gw(e,n){return e===null?[n]:Array.isArray(e)?[...e,n]:[e,n]}function s0(e){return e._rawValidators}function a0(e){return e._rawAsyncValidators}function tp(e){return e?Array.isArray(e)?e:[e]:[]}function id(e,n){return Array.isArray(e)?e.includes(n):e===n}function Ww(e,n){let t=tp(n);return tp(e).forEach(r=>{id(t,r)||t.push(r)}),t}function qw(e,n){return tp(n).filter(t=>!id(e,t))}var rd=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=op(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=sp(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,t){return this.control?this.control.hasError(n,t):!1}getError(n,t){return this.control?this.control.getError(n,t):null}},ho=class extends rd{name;get formDirective(){return null}get path(){return null}},po=class extends rd{_parent=null;name=null;valueAccessor=null};var Gs="VALID",nd="INVALID",uo="PENDING",Ws="DISABLED",bi=class{},od=class extends bi{value;source;constructor(n,t){super(),this.value=n,this.source=t}},Ys=class extends bi{pristine;source;constructor(n,t){super(),this.pristine=n,this.source=t}},Qs=class extends bi{touched;source;constructor(n,t){super(),this.touched=n,this.source=t}},fo=class extends bi{status;source;constructor(n,t){super(),this.status=n,this.source=t}},sd=class extends bi{source;constructor(n){super(),this.source=n}},ad=class extends bi{source;constructor(n){super(),this.source=n}};function c0(e){return(hd(e)?e.validators:e)||null}function tR(e){return Array.isArray(e)?op(e):e||null}function l0(e,n){return(hd(n)?n.asyncValidators:e)||null}function nR(e){return Array.isArray(e)?sp(e):e||null}function hd(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function iR(e,n,t){let i=e.controls;if(!(n?Object.keys(i):i).length)throw new x(1e3,"");if(!i[t])throw new x(1001,"")}function rR(e,n,t){e._forEachChild((i,r)=>{if(t[r]===void 0)throw new x(-1002,"")})}var cd=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,t){this._assignValidators(n),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Le(this.statusReactive)}set status(n){Le(()=>this.statusReactive.set(n))}_status=xe(()=>this.statusReactive());statusReactive=O(void 0);get valid(){return this.status===Gs}get invalid(){return this.status===nd}get pending(){return this.status===uo}get disabled(){return this.status===Ws}get enabled(){return this.status!==Ws}errors;get pristine(){return Le(this.pristineReactive)}set pristine(n){Le(()=>this.pristineReactive.set(n))}_pristine=xe(()=>this.pristineReactive());pristineReactive=O(!0);get dirty(){return!this.pristine}get touched(){return Le(this.touchedReactive)}set touched(n){Le(()=>this.touchedReactive.set(n))}_touched=xe(()=>this.touchedReactive());touchedReactive=O(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(Ww(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(Ww(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(qw(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(qw(n,this._rawAsyncValidators))}hasValidator(n){return id(this._rawValidators,n)}hasAsyncValidator(n){return id(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let t=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(R(_({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new Qs(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(n))}markAsUntouched(n={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),t&&n.emitEvent!==!1&&this._events.next(new Qs(!1,i))}markAsDirty(n={}){let t=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(R(_({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new Ys(!1,i))}markAsPristine(n={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),t&&n.emitEvent!==!1&&this._events.next(new Ys(!0,i))}markAsPending(n={}){this.status=uo;let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new fo(this.status,t)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(R(_({},n),{sourceControl:t}))}disable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=Ws,this.errors=null,this._forEachChild(r=>{r.disable(R(_({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new od(this.value,i)),this._events.next(new fo(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(R(_({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=Gs,this._forEachChild(i=>{i.enable(R(_({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(R(_({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,t){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Gs||this.status===uo)&&this._runAsyncValidator(i,n.emitEvent)}let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new od(this.value,t)),this._events.next(new fo(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(R(_({},n),{sourceControl:t}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ws:Gs}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,t){if(this.asyncValidator){this.status=uo,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:n!==!1};let i=e0(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,t={}){this.errors=n,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(n){let t=n;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(n,t){let i=t?this.get(t):this;return i?.errors?i.errors[n]:null}hasError(n,t){return!!this.getError(n,t)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,t,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new fo(this.status,t)),this._parent&&this._parent._updateControlsErrors(n,t,i)}_initObservables(){this.valueChanges=new j,this.statusChanges=new j}_calculateStatus(){return this._allControlsDisabled()?Ws:this.errors?nd:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(uo)?uo:this._anyControlsHaveStatus(nd)?nd:Gs}_anyControlsHaveStatus(n){return this._anyControls(t=>t.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,t),r&&this._events.next(new Ys(this.pristine,t))}_updateTouched(n={},t){this.touched=this._anyControlsTouched(),this._events.next(new Qs(this.touched,t)),n.onlySelf||this._parent?._updateTouched(n,t)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){hd(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=tR(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=nR(this._rawAsyncValidators)}},ld=class extends cd{constructor(n,t,i){super(c0(t),l0(i,t)),this.controls=n,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,t){return this.controls[n]?this.controls[n]:(this.controls[n]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(n,t,i={}){this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,t={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(n,t,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],t&&this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,t={}){rR(this,!0,n),Object.keys(n).forEach(i=>{iR(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)}patchValue(n,t={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(n={},t={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,R(_({},t),{onlySelf:!0}))}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t),t?.emitEvent!==!1&&this._events.next(new ad(this))}getRawValue(){return this._reduceChildren({},(n,t,i)=>(n[i]=t.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(t,i)=>i._syncPendingControls()?!0:t);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(t=>{let i=this.controls[t];i&&n(i,t)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[t,i]of Object.entries(this.controls))if(this.contains(t)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(t,i,r)=>((i.enabled||this.disabled)&&(t[r]=i.value),t))}_reduceChildren(n,t){let i=n;return this._forEachChild((r,o)=>{i=t(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var d0=new y("",{factory:()=>u0}),u0="always";function np(e,n,t=u0){ap(e,n),n.valueAccessor.writeValue(e.value),(e.disabled||t==="always")&&n.valueAccessor.setDisabledState?.(e.disabled),sR(e,n),cR(e,n),aR(e,n),oR(e,n)}function Yw(e,n,t=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),ud(e,n),e&&(n._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function dd(e,n){e.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(n)})}function oR(e,n){if(n.valueAccessor.setDisabledState){let t=i=>{n.valueAccessor.setDisabledState(i)};e.registerOnDisabledChange(t),n._registerOnDestroy(()=>{e._unregisterOnDisabledChange(t)})}}function ap(e,n){let t=s0(e);n.validator!==null?e.setValidators(Gw(t,n.validator)):typeof t=="function"&&e.setValidators([t]);let i=a0(e);n.asyncValidator!==null?e.setAsyncValidators(Gw(i,n.asyncValidator)):typeof i=="function"&&e.setAsyncValidators([i]);let r=()=>e.updateValueAndValidity();dd(n._rawValidators,r),dd(n._rawAsyncValidators,r)}function ud(e,n){let t=!1;if(e!==null){if(n.validator!==null){let r=s0(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(t=!0,e.setValidators(o))}}if(n.asyncValidator!==null){let r=a0(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(t=!0,e.setAsyncValidators(o))}}}let i=()=>{};return dd(n._rawValidators,i),dd(n._rawAsyncValidators,i),t}function sR(e,n){n.valueAccessor.registerOnChange(t=>{e._pendingValue=t,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&f0(e,n)})}function aR(e,n){n.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&f0(e,n),e.updateOn!=="submit"&&e.markAsTouched()})}function f0(e,n){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function cR(e,n){let t=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};e.registerOnChange(t),n._registerOnDestroy(()=>{e._unregisterOnChange(t)})}function m0(e,n){e==null,ap(e,n)}function lR(e,n){return ud(e,n)}function h0(e,n){e._syncPendingControls(),n.forEach(t=>{let i=t.control;i.updateOn==="submit"&&i._pendingChange&&(t.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function dR(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}var uR={provide:ho,useExisting:ht(()=>Ks)},qs=Promise.resolve(),Ks=(()=>{class e extends ho{callSetDisabledState;get submitted(){return Le(this.submittedReactive)}_submitted=xe(()=>this.submittedReactive());submittedReactive=O(!1);_directives=new Set;form;ngSubmit=new j;options;constructor(t,i,r){super(),this.callSetDisabledState=r,this.form=new ld({},op(t),sp(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){qs.then(()=>{let i=this._findContainer(t.path);t.control=i.registerControl(t.name,t.control),np(t.control,t,this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){qs.then(()=>{this._findContainer(t.path)?.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){qs.then(()=>{let i=this._findContainer(t.path),r=new ld({});m0(r,t),i.registerControl(t.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){qs.then(()=>{this._findContainer(t.path)?.removeControl?.(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,i){qs.then(()=>{this.form.get(t.path).setValue(i)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),h0(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new sd(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(i){return new(i||e)(dt(md,10),dt(Zw,10),dt(d0,8))};static \u0275dir=G({type:e,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&k("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Pe([uR]),ut]})}return e})();function Qw(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function Kw(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var fR=class extends cd{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,t,i){super(c0(t),l0(i,t)),this._applyFormState(n),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),hd(t)&&(t.nonNullable||t.initialValueIsDefault)&&(Kw(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,t={}){this.value=this._pendingValue=n,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(n,t={}){this.setValue(n,t)}reset(n=this.defaultValue,t={}){this._applyFormState(n),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new ad(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){Qw(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){Qw(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){Kw(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var mR=e=>e instanceof fR;var hR=(()=>{class e extends ho{callSetDisabledState;get submitted(){return Le(this._submittedReactive)}set submitted(t){this._submittedReactive.set(t)}_submitted=xe(()=>this._submittedReactive());_submittedReactive=O(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(t,i,r){super(),this.callSetDisabledState=r,this._setValidators(t),this._setAsyncValidators(i)}ngOnChanges(t){this.onChanges(t)}ngOnDestroy(){this.onDestroy()}onChanges(t){this._checkFormPresent(),t.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(ud(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(t){let i=this.form.get(t.path);return np(i,t,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(t),i}getControl(t){return this.form.get(t.path)}removeControl(t){Yw(t.control||null,t,!1),dR(this.directives,t)}addFormGroup(t){this._setUpFormContainer(t)}removeFormGroup(t){this._cleanUpFormContainer(t)}getFormGroup(t){return this.form.get(t.path)}getFormArray(t){return this.form.get(t.path)}addFormArray(t){this._setUpFormContainer(t)}removeFormArray(t){this._cleanUpFormContainer(t)}updateModel(t,i){this.form.get(t.path).setValue(i)}onReset(){this.resetForm()}resetForm(t=void 0,i={}){this.form.reset(t,i),this._submittedReactive.set(!1)}onSubmit(t){return this.submitted=!0,h0(this.form,this.directives),this.ngSubmit.emit(t),this.form._events.next(new sd(this.control)),t?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(t=>{let i=t.control,r=this.form.get(t.path);i!==r&&(Yw(i||null,t),mR(r)&&(np(r,t,this.callSetDisabledState),t.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(t){let i=this.form.get(t.path);m0(i,t),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(t){let i=this.form?.get(t.path);i&&lR(i,t)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){ap(this.form,this),this._oldForm&&ud(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||e)(dt(md,10),dt(Zw,10),dt(d0,8))};static \u0275dir=G({type:e,features:[ut,Oe]})}return e})();var pR={provide:ho,useExisting:ht(()=>Zs)},Zs=(()=>{class e extends hR{form=null;ngSubmit=new j;get control(){return this.form}static \u0275fac=(()=>{let t;return function(r){return(t||(t=kt(e)))(r||e)}})();static \u0275dir=G({type:e,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&k("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Pe([pR]),ut]})}return e})();var p0=new y("MAT_INPUT_VALUE_ACCESSOR");var cp=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ee(t=>{let i=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(we(t=>t.some(i=>i.target===n)),Xa({bufferSize:1,refCount:!0}),$e(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},g0=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=u(T);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new cp(r)),this._observers.get(r).observe(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var gR=new y("MATERIAL_ANIMATIONS"),v0=null;function vR(){return u(gR,{optional:!0})?.animationsDisabled||u(as,{optional:!0})==="NoopAnimations"?"di-disabled":(v0??=u(Qh).matchMedia("(prefers-reduced-motion)").matches,v0?"reduced-motion":"enabled")}function it(){return vR()!=="enabled"}var _R=["notch"],yR=["matFormFieldNotchedOutline",""],bR=["*"],_0=["iconPrefixContainer"],y0=["textPrefixContainer"],b0=["iconSuffixContainer"],w0=["textSuffixContainer"],wR=["textField"],CR=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],DR=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function xR(e,n){e&1&&le(0,"span",21)}function ER(e,n){if(e&1&&(p(0,"label",20),te(1,1),me(2,xR,1,0,"span",21),v()),e&2){let t=ie(2);N("floating",t._shouldLabelFloat())("monitorResize",t._hasOutline())("id",t._labelId),ge("for",t._control.disableAutomaticLabeling?null:t._control.id),b(2),he(!t.hideRequiredMarker&&t._control.required?2:-1)}}function IR(e,n){if(e&1&&me(0,ER,3,5,"label",20),e&2){let t=ie();he(t._hasFloatingLabel()?0:-1)}}function SR(e,n){e&1&&le(0,"div",7)}function MR(e,n){}function kR(e,n){if(e&1&&mn(0,MR,0,0,"ng-template",13),e&2){ie(2);let t=li(1);N("ngTemplateOutlet",t)}}function TR(e,n){if(e&1&&(p(0,"div",9),me(1,kR,1,1,null,13),v()),e&2){let t=ie();N("matFormFieldNotchedOutlineOpen",t._shouldLabelFloat()),b(),he(t._forceDisplayInfixLabel()?-1:1)}}function RR(e,n){e&1&&(p(0,"div",10,2),te(2,2),v())}function AR(e,n){e&1&&(p(0,"div",11,3),te(2,3),v())}function NR(e,n){}function OR(e,n){if(e&1&&mn(0,NR,0,0,"ng-template",13),e&2){ie();let t=li(1);N("ngTemplateOutlet",t)}}function FR(e,n){e&1&&(p(0,"div",14,4),te(2,4),v())}function PR(e,n){e&1&&(p(0,"div",15,5),te(2,5),v())}function LR(e,n){e&1&&le(0,"div",16)}function VR(e,n){e&1&&(p(0,"div",18),te(1,6),v())}function jR(e,n){if(e&1&&(p(0,"mat-hint",22),D(1),v()),e&2){let t=ie(2);N("id",t._hintLabelId),b(),Hn(t.hintLabel)}}function BR(e,n){if(e&1&&(p(0,"div",19),me(1,jR,2,2,"mat-hint",22),te(2,7),le(3,"div",23),te(4,8),v()),e&2){let t=ie();b(),he(t.hintLabel?1:-1)}}var Cn=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-label"]]})}return e})(),HR=new y("MatError");var gd=(()=>{class e{align="start";id=u(ze).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Zt("id",r.id),ge("align",null),B("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),UR=new y("MatPrefix");var zR=new y("MatSuffix");var M0=new y("FloatingLabelParent"),C0=(()=>{class e{_elementRef=u($);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(g0);_ngZone=u(T);_parent=u(M0);_resizeSubscription=new ae;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return $R(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function $R(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let i=t.scrollWidth;return t.remove(),i}var D0="mdc-line-ripple--active",pd="mdc-line-ripple--deactivating",x0=(()=>{class e{_elementRef=u($);_cleanupTransitionEnd;constructor(){let t=u(T),i=u(Fe);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove(pd),t.add(D0)}deactivate(){this._elementRef.nativeElement.classList.add(pd)}_handleTransitionEnd=t=>{let i=this._elementRef.nativeElement.classList,r=i.contains(pd);t.propertyName==="opacity"&&r&&i.remove(D0,pd)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),E0=(()=>{class e{_elementRef=u($);_ngZone=u(T);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,i=t.querySelector(".mdc-floating-label");i?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let i=this._notch.nativeElement;!this.open||!t?i.style.width="":i.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&et(_R,5),i&2){let o;re(o=oe())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:yR,ngContentSelectors:bR,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(De(),hn(0,"div",1),At(1,"div",2,0),te(3),Nt(),hn(4,"div",3))},encapsulation:2,changeDetection:0})}return e})(),Xs=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e})}return e})();var Js=new y("MatFormField"),GR=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),I0="fill",WR="auto",S0="fixed",qR="translateY(-50%)",Yn=(()=>{class e{_elementRef=u($);_changeDetectorRef=u(He);_platform=u(Ee);_idGenerator=u(ze);_ngZone=u(T);_defaults=u(GR,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ps("iconPrefixContainer");_textPrefixContainerSignal=ps("textPrefixContainer");_iconSuffixContainerSignal=ps("iconSuffixContainer");_textSuffixContainerSignal=ps("textSuffixContainer");_prefixSuffixContainers=xe(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=rb(Cn);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=qn(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||WR}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let i=t||this._defaults?.appearance||I0;this._appearanceSignal.set(i)}_appearanceSignal=O(I0);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||S0}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||S0}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=it();constructor(){let t=this._defaults,i=u(Wn);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),On(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=xe(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let i=this._control,r="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(r+t.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(In([void 0,void 0]),ce(()=>[i.errorState,i.userAriaDescribedBy]),Za(),we(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe($e(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Li(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._control.focused;t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){ab({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=xe(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(t){let i=this._control?this._control.ngControl:null;return i&&i[t]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?t.push(o.id):this._hintLabel&&t.push(this._hintLabelId),s&&t.push(s.id)}else this._errorChildren&&t.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||t;r=t.concat(i.filter(s=>s&&!o.includes(s)))}else r=t;this._control.setDescribedByIds(r),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=t?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,h=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,g=`var(--mat-mdc-form-field-label-transform, ${qR} translateX(${h}))`,C=s+a+c+l;return[g,C]}_writeOutlinedLabelStyles(t){if(t!==null){let[i,r]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let i=t.getRootNode();return i&&i!==t}return document.documentElement.contains(t)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(fl(o,r._labelChild,Cn,5),Xt(o,Xs,5)(o,UR,5)(o,zR,5)(o,HR,5)(o,gd,5)),i&2){hl();let s;re(s=oe())&&(r._formFieldControl=s.first),re(s=oe())&&(r._prefixChildren=s),re(s=oe())&&(r._suffixChildren=s),re(s=oe())&&(r._errorChildren=s),re(s=oe())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(ml(r._iconPrefixContainerSignal,_0,5)(r._textPrefixContainerSignal,y0,5)(r._iconSuffixContainerSignal,b0,5)(r._textSuffixContainerSignal,w0,5),et(wR,5)(_0,5)(y0,5)(b0,5)(w0,5)(C0,5)(E0,5)(x0,5)),i&2){hl(4);let o;re(o=oe())&&(r._textField=o.first),re(o=oe())&&(r._iconPrefixContainer=o.first),re(o=oe())&&(r._textPrefixContainer=o.first),re(o=oe())&&(r._iconSuffixContainer=o.first),re(o=oe())&&(r._textSuffixContainer=o.first),re(o=oe())&&(r._floatingLabel=o.first),re(o=oe())&&(r._notchedOutline=o.first),re(o=oe())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&B("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Pe([{provide:Js,useExisting:e},{provide:M0,useExisting:e}])],ngContentSelectors:DR,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(De(CR),mn(0,IR,1,1,"ng-template",null,0,$m),p(2,"div",6,1),k("click",function(s){return r._control.onContainerClick(s)}),me(4,SR,1,0,"div",7),p(5,"div",8),me(6,TR,2,2,"div",9),me(7,RR,3,0,"div",10),me(8,AR,3,0,"div",11),p(9,"div",12),me(10,OR,1,1,null,13),te(11),v(),me(12,FR,3,0,"div",14),me(13,PR,3,0,"div",15),v(),me(14,LR,1,0,"div",16),v(),p(15,"div",17),me(16,VR,2,0,"div",18)(17,BR,5,1,"div",19),v()),i&2){let o;b(2),B("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),b(2),he(!r._hasOutline()&&!r._control.disabled?4:-1),b(2),he(r._hasOutline()?6:-1),b(),he(r._hasIconPrefix?7:-1),b(),he(r._hasTextPrefix?8:-1),b(2),he(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),b(2),he(r._hasTextSuffix?12:-1),b(),he(r._hasIconSuffix?13:-1),b(),he(r._hasOutline()?-1:14),b(),B("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();b(),he((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[C0,E0,Xm,x0,gd],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return e})();var go=(()=>{class e{isErrorState(t,i){return!!(t&&t.invalid&&(t.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var vo=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,t,i,r,o){this._defaultMatcher=n,this.ngControl=t,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,t=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,t)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var en=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[Pw,Yn,pe]})}return e})();var ZR=["button","checkbox","file","hidden","image","radio","range","reset","submit"],XR=new y("MAT_INPUT_CONFIG"),_o=(()=>{class e{_elementRef=u($);_platform=u(Ee);ngControl=u(po,{optional:!0,self:!0});_autofillMonitor=u(Sw);_ngZone=u(T);_formField=u(Js,{optional:!0});_renderer=u(Fe);_uid=u(ze).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(XR,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=qn(t),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(t){this._id=t||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(mo.required)??!1}set required(t){this._required=qn(t)}_required;get type(){return this._type}set type(t){this._type=t||"text",this._validateType(),!this._isTextarea&&Wh().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(t){this._errorStateTracker.matcher=t}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(t){t!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(t):this._inputValueAccessor.value=t,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(t){this._readonly=qn(t)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(t){this._errorStateTracker.errorState=t}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(t=>Wh().has(t));constructor(){let t=u(Ks,{optional:!0}),i=u(Zs,{optional:!0}),r=u(go),o=u(p0,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?Bn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new vo(r,this.ngControl,i,t,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&On(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(t=>{this.autofilled=t.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(t){this._elementRef.nativeElement.focus(t)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(t){if(t!==this.focused){if(!this._isNativeSelect&&t&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=t,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let t=this._elementRef.nativeElement.value;this._previousNativeValue!==t&&(this._previousNativeValue=t,this.stateChanges.next())}_dirtyCheckPlaceholder(){let t=this._getPlaceholder();if(t!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=t,t?i.setAttribute("placeholder",t):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){ZR.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let t=this._elementRef.nativeElement.validity;return t&&t.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let t=this._elementRef.nativeElement,i=t.options[0];return this.focused||t.multiple||!this.empty||!!(t.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(t){let i=this._elementRef.nativeElement;t.length?i.setAttribute("aria-describedby",t.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let t=this._elementRef.nativeElement;return this._isNativeSelect&&(t.multiple||t.size>1)}_iOSKeyupListener=t=>{let i=t.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&k("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Zt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ge("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),B("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},exportAs:["matInput"],features:[Pe([{provide:Xs,useExisting:e}]),Oe]})}return e})(),wi=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[en,en,Mw,pe]})}return e})();var ea=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;constructor(n=!1,t,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,t&&t.length&&(n?t.forEach(o=>this._markSelected(o)):this._markSelected(t[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let t=this._hasQueuedChanges();return this._emitChangeEvent(),t}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let t=this._hasQueuedChanges();return this._emitChangeEvent(),t}setSelection(...n){this._verifyValueAssignment(n);let t=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),t.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let t=this._hasQueuedChanges();return n&&this._emitChangeEvent(),t}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,t){if(this.compareWith){t=t??this._selection;for(let i of t)if(this.compareWith(n,i))return i;return n}else return n}};var dp=(()=>{class e{_listeners=[];notify(t,i){for(let r of this._listeners)r(t,i)}listen(t){return this._listeners.push(t),()=>{this._listeners=this._listeners.filter(i=>t!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var JR=["mat-internal-form-field",""],eA=["*"],vd=(()=>{class e{labelPosition="after";static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&B("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:JR,ngContentSelectors:eA,decls:1,vars:0,template:function(i,r){i&1&&(De(),te(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var Ci=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var Vt=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(Vt||{}),up=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Vt.HIDDEN;constructor(n,t,i,r=!1){this._renderer=n,this.element=t,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},k0=co({passive:!0,capture:!0}),fp=class{_events=new Map;addHandler(n,t,i,r){let o=this._events.get(t);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(t,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,k0)})}removeHandler(n,t,i){let r=this._events.get(n);if(!r)return;let o=r.get(t);o&&(o.delete(i),o.size===0&&r.delete(t),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,k0)))}_delegateEventHandler=n=>{let t=We(n);t&&this._events.get(n.type)?.forEach((i,r)=>{(r===t||r.contains(t))&&i.forEach(o=>o.handleEvent(n))})}},ta={enterDuration:225,exitDuration:150},tA=800,T0=co({passive:!0,capture:!0}),R0=["mousedown","touchstart"],A0=["mouseup","mouseleave","touchend","touchcancel"],nA=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return e})(),na=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new fp;constructor(n,t,i,r,o){this._target=n,this._ngZone=t,this._platform=r,r.isBrowser&&(this._containerElement=Dt(i)),o&&o.get(nt).load(nA)}fadeInRipple(n,t,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=_(_({},ta),i.animation);i.centered&&(n=r.left+r.width/2,t=r.top+r.height/2);let s=i.radius||iA(n,t,r),a=n-r.left,c=t-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),m=f.transitionProperty,h=f.transitionDuration,g=m==="none"||h==="0s"||h==="0s, 0s"||r.width===0&&r.height===0,C=new up(this,d,i,g);d.style.transform="scale3d(1, 1, 1)",C.state=Vt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=C);let W=null;return!g&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ne=()=>{W&&(W.fallbackTimer=null),clearTimeout(jt),this._finishRippleTransition(C)},Re=()=>this._destroyRipple(C),jt=setTimeout(Re,l+100);d.addEventListener("transitionend",ne),d.addEventListener("transitioncancel",Re),W={onTransitionEnd:ne,onTransitionCancel:Re,fallbackTimer:jt}}),this._activeRipples.set(C,W),(g||!l)&&this._finishRippleTransition(C),C}fadeOutRipple(n){if(n.state===Vt.FADING_OUT||n.state===Vt.HIDDEN)return;let t=n.element,i=_(_({},ta),n.config.animation);t.style.transitionDuration=`${i.exitDuration}ms`,t.style.opacity="0",n.state=Vt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=Dt(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,R0.forEach(i=>{e._eventManager.addHandler(this._ngZone,i,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{A0.forEach(t=>{this._triggerElement.addEventListener(t,this,T0)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Vt.FADING_IN?this._startFadeOutTransition(n):n.state===Vt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Vt.VISIBLE,!i&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Vt.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=Hs(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+tA;!this._target.rippleDisabled&&!t&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Us(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let i=0;i<t.length;i++)this.fadeInRipple(t[i].clientX,t[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===Vt.VISIBLE||n.config.terminateOnPointerUp&&n.state===Vt.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(R0.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(A0.forEach(t=>n.removeEventListener(t,this,T0)),this._pointerUpEventsRegistered=!1))}};function iA(e,n,t){let i=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),r=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(i*i+r*r)}var mp=new y("mat-ripple-global-options"),yo=(()=>{class e{_elementRef=u($);_animationsDisabled=it();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(t){t&&this.fadeOutAllNonPersistent(),this._disabled=t,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(t){this._trigger=t,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let t=u(T),i=u(Ee),r=u(mp,{optional:!0}),o=u(J);this._globalOptions=r||{},this._rippleRenderer=new na(this,t,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:_(_(_({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(t,i=0,r){return typeof t=="number"?this._rippleRenderer.fadeInRipple(t,i,_(_({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,_(_({},this.rippleConfig),t))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&B("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return e})();var Di=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[pe]})}return e})();var rA=["input"],oA=["formField"],sA=["*"],_d=class{source;value;constructor(n,t){this.source=n,this.value=t}},aA={provide:fd,useExisting:ht(()=>ia),multi:!0},N0=new y("MatRadioGroup"),cA=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),ia=(()=>{class e{_changeDetector=u(He);_value=null;_name=u(ze).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new j;_radios;color;get name(){return this._name}set name(t){this._name=t,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(t){this._labelPosition=t==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(t){this._value!==t&&(this._value=t,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(t){this._selected=t,this.value=t?t.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._markRadiosForCheck()}get required(){return this._required}set required(t){this._required=t,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(t){this._disabledInteractive=t,this._markRadiosForCheck()}_disabledInteractive=!1;constructor(){}ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(t=>t===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(t=>{t.name=this.name,t._markForCheck()})}_updateSelectedRadioFromValue(){let t=this._selected!==null&&this._selected.value===this._value;this._radios&&!t&&(this._selected=null,this._radios.forEach(i=>{i.checked=this.value===i.value,i.checked&&(this._selected=i)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new _d(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(t=>t._markForCheck())}writeValue(t){this.value=t,this._changeDetector.markForCheck()}registerOnChange(t){this._controlValueAccessorChangeFn=t}registerOnTouched(t){this.onTouched=t}setDisabledState(t){this.disabled=t,this._changeDetector.markForCheck()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["mat-radio-group"]],contentQueries:function(i,r,o){if(i&1&&Xt(o,bo,5),i&2){let s;re(s=oe())&&(r._radios=s)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",P],required:[2,"required","required",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[Pe([aA,{provide:N0,useExisting:e}])]})}return e})(),bo=(()=>{class e{_elementRef=u($);_changeDetector=u(He);_focusMonitor=u(mr);_radioDispatcher=u(dp);_defaultOptions=u(cA,{optional:!0});_ngZone=u(T);_renderer=u(Fe);_uniqueId=u(ze).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(t){this._checked!==t&&(this._checked=t,t&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!t&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),t&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(t){this._value!==t&&(this._value=t,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===t),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(t){this._labelPosition=t}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(t){this._setDisabled(t)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(t){t!==this._required&&this._changeDetector.markForCheck(),this._required=t}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(t){this._color=t}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(t){this._disabledInteractive=t}_disabledInteractive;change=new j;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=it();_injector=u(J);constructor(){u(nt).load(Ci);let t=u(N0,{optional:!0}),i=u(new Un("tabindex"),{optional:!0});this.radioGroup=t,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=Ft(i,0))}focus(t,i){i?this._focusMonitor.focusVia(this._inputElement,i,t):this._inputElement.nativeElement.focus(t)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((t,i)=>{t!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{!t&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new _d(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(t){if(t.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_onTouchTargetClick(t){this._onInputInteraction(t),(!this.disabled||this.disabledInteractive)&&this._inputElement?.nativeElement.focus()}_setDisabled(t){this._disabled!==t&&(this._disabled=t,this._changeDetector.markForCheck())}_onInputClick=t=>{this.disabled&&this.disabledInteractive&&t.preventDefault()};_updateTabIndex(){let t=this.radioGroup,i;if(!t||!t.selected||this.disabled?i=this.tabIndex:i=t.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,Tt(()=>{queueMicrotask(()=>{t&&t.selected&&t.selected!==this&&document.activeElement===r&&(t.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&et(rA,5)(oA,7,$),i&2){let o;re(o=oe())&&(r._inputElement=o.first),re(o=oe())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&k("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(ge("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),B("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",P],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Ft(t)],checked:[2,"checked","checked",P],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",P],required:[2,"required","required",P],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:sA,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition"],[1,"mdc-radio"],["aria-hidden","true",1,"mat-mdc-radio-touch-target",3,"click"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mdc-label",3,"for"]],template:function(i,r){i&1&&(De(),p(0,"div",2,0)(2,"div",3)(3,"div",4),k("click",function(s){return r._onTouchTargetClick(s)}),v(),p(4,"input",5,1),k("change",function(s){return r._onInputInteraction(s)}),v(),p(6,"div",6),le(7,"div",7)(8,"div",8),v(),p(9,"div",9),le(10,"div",10),v()(),p(11,"label",11),te(12),v()()),i&2&&(N("labelPosition",r.labelPosition),b(2),B("mdc-radio--disabled",r.disabled),b(2),N("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ge("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),b(5),N("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0),b(2),N("for",r.inputId))},dependencies:[yo,vd],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-hover-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-pressed-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button label {
  cursor: pointer;
}
.mat-mdc-radio-button label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--mat-radio-checked-ripple-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--mat-radio-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-radio-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-radio-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-radio-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-radio-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-radio-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-radio-button .mdc-radio--disabled + label {
  color: var(--mat-radio-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-radio-touch-target-size, 48px);
  width: var(--mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return e})(),xi=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[Di,bo,pe]})}return e})();var lA={capture:!0},dA=["focus","mousedown","mouseenter","touchstart"],hp="mat-ripple-loader-uninitialized",pp="mat-ripple-loader-class-name",F0="mat-ripple-loader-centered",yd="mat-ripple-loader-disabled",P0=(()=>{class e{_document=u(Y);_animationsDisabled=it();_globalRippleOptions=u(mp,{optional:!0});_platform=u(Ee);_ngZone=u(T);_injector=u(J);_eventCleanups;_hosts=new Map;constructor(){let t=u(Ne).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>dA.map(i=>t.listen(this._document,i,this._onInteraction,lA)))}ngOnDestroy(){let t=this._hosts.keys();for(let i of t)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(t,i){t.setAttribute(hp,this._globalRippleOptions?.namespace??""),(i.className||!t.hasAttribute(pp))&&t.setAttribute(pp,i.className||""),i.centered&&t.setAttribute(F0,""),i.disabled&&t.setAttribute(yd,"")}setDisabled(t,i){let r=this._hosts.get(t);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(t))):i?t.setAttribute(yd,""):t.removeAttribute(yd)}_onInteraction=t=>{let i=We(t);if(i instanceof HTMLElement){let r=i.closest(`[${hp}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",t.getAttribute(pp)),t.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??ta.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??ta.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||t.hasAttribute(yd),rippleConfig:{centered:t.hasAttribute(F0),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new na(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute(hp)}destroyRipple(t){let i=this._hosts.get(t);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var uA=new y("MAT_BUTTON_CONFIG");function L0(e){return e==null?void 0:Ft(e)}var V0=(()=>{class e{_elementRef=u($);_ngZone=u(T);_animationsDisabled=it();_config=u(uA,{optional:!0});_focusMonitor=u(mr);_cleanupClick;_renderer=u(Fe);_rippleLoader=u(P0);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}constructor(){u(nt).load(Ci);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",i){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(ge("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),di(r.color?"mat-"+r.color:""),B("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",P],disabled:[2,"disabled","disabled",P],ariaDisabled:[2,"aria-disabled","ariaDisabled",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P],tabIndex:[2,"tabIndex","tabIndex",L0],_tabindex:[2,"tabindex","_tabindex",L0]}})}return e})();var fA=["matButton",""],mA=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],hA=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var j0=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Ei=(()=>{class e extends V0{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=pA(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?j0.get(this._appearance):null,o=j0.get(t);r&&i.remove(...r),i.add(...o),this._appearance=t}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[ut],attrs:fA,ngContentSelectors:hA,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(mA),hn(0,"span",0),te(1),At(2,"span",1),te(3,1),Nt(),te(4,2),hn(5,"span",2)(6,"span",3)),i&2&&B("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function pA(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var Dn=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[Di,pe]})}return e})();var gA=20,vp=(()=>{class e{_ngZone=u(T);_platform=u(Ee);_renderer=u(Ne).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(t){this.scrollContainers.has(t)||this.scrollContainers.set(t,t.elementScrolled().subscribe(()=>this._scrolled.next(t)))}deregister(t){let i=this.scrollContainers.get(t);i&&(i.unsubscribe(),this.scrollContainers.delete(t))}scrolled(t=gA){return this._platform.isBrowser?new ee(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=t>0?this._scrolled.pipe(qa(t)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Z()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((t,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(t,i){let r=this.getAncestorScrollContainers(t);return this.scrolled(i).pipe(we(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(t){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,t)&&i.push(o)}),i}_scrollableContainsElement(t,i){let r=Dt(i),o=t.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var vA=20,Ii=(()=>{class e{_platform=u(Ee);_listeners;_viewportSize=null;_change=new E;_document=u(Y);constructor(){let t=u(T),i=u(Ne).createRenderer(null,null);t.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(t=>t()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let t={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),t}getViewportRect(){let t=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:t.top,left:t.left,bottom:t.top+r,right:t.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let t=this._document,i=this._getWindow(),r=t.documentElement,o=r.getBoundingClientRect(),s=-o.top||t.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||t.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(t=vA){return t>0?this._change.pipe(qa(t)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let t=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:t.innerWidth,height:t.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var bd=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({})}return e})(),ra=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[pe,bd,pe,bd]})}return e})();var oa=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},_p=class extends oa{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,t,i,r,o){super(),this.component=n,this.viewContainerRef=t,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},sa=class extends oa{templateRef;viewContainerRef;context;injector;constructor(n,t,i,r){super(),this.templateRef=n,this.viewContainerRef=t,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,t=this.context){return this.context=t,super.attach(n)}detach(){return this.context=void 0,super.detach()}},yp=class extends oa{element;constructor(n){super(),this.element=n instanceof $?n.nativeElement:n}},bp=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof _p)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof sa)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof yp)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},wd=class extends bp{outletElement;_appRef;_defaultInjector;constructor(n,t,i){super(),this.outletElement=n,this._appRef=t,this._defaultInjector=i}attachComponentPortal(n){let t;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Pn,null,{optional:!0})||void 0;t=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>t.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||J.NULL,o=r.get(ye,i.injector);t=yl(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(t.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(t.hostView),t.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(t)),this._attachedPortal=n,t}attachTemplatePortal(n){let t=n.viewContainerRef,i=t.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=t.indexOf(i);r!==-1&&t.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let t=n.element;t.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");t.parentNode.insertBefore(i,t),this.outletElement.appendChild(t),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(t,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var B0=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({})}return e})();var H0=Dw();function Q0(e){return new Cd(e.get(Ii),e.get(Y))}var Cd=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,t){this._viewportRuler=n,this._document=t}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Te(-this._previousScrollPosition.left),n.style.top=Te(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,t=this._document.body,i=n.style,r=t.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),H0&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),H0&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let t=this._document.documentElement,i=this._viewportRuler.getViewportSize();return t.scrollHeight>i.height||t.scrollWidth>i.width}};function K0(e,n){return new Dd(e.get(vp),e.get(T),e.get(Ii),n)}var Dd=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,t,i,r){this._scrollDispatcher=n,this._ngZone=t,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(we(t=>!t||!this._overlayRef.overlayElement.contains(t.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let t=this._viewportRuler.getViewportScrollPosition().top;Math.abs(t-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var aa=class{enable(){}disable(){}attach(){}};function wp(e,n){return n.some(t=>{let i=e.bottom<t.top,r=e.top>t.bottom,o=e.right<t.left,s=e.left>t.right;return i||r||o||s})}function U0(e,n){return n.some(t=>{let i=e.top<t.top,r=e.bottom>t.bottom,o=e.left<t.left,s=e.right>t.right;return i||r||o||s})}function la(e,n){return new xd(e.get(vp),e.get(Ii),e.get(T),n)}var xd=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,t,i,r){this._scrollDispatcher=n,this._viewportRuler=t,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let t=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();wp(t,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},Z0=(()=>{class e{_injector=u(J);constructor(){}noop=()=>new aa;close=t=>K0(this._injector,t);block=()=>Q0(this._injector);reposition=t=>la(this._injector,t);static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),ca=class{positionStrategy;scrollStrategy=new aa;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let t=Object.keys(n);for(let i of t)n[i]!==void 0&&(this[i]=n[i])}}};var Ed=class{connectionPair;scrollableViewProperties;constructor(n,t){this.connectionPair=n,this.scrollableViewProperties=t}};var X0=(()=>{class e{_attachedOverlays=[];_document=u(Y);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(t){this.remove(t),this._attachedOverlays.push(t)}remove(t){let i=this._attachedOverlays.indexOf(t);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(t,i,r){return r.observers.length<1?!1:t.eventPredicate?t.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),J0=(()=>{class e extends X0{_ngZone=u(T);_renderer=u(Ne).createRenderer(null,null);_cleanupKeydown;add(t){super.add(t),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=t=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,t,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(t));break}}};static \u0275fac=(()=>{let t;return function(r){return(t||(t=kt(e)))(r||e)}})();static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),eC=(()=>{class e extends X0{_platform=u(Ee);_ngZone=u(T);_renderer=u(Ne).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(t){if(super.add(t),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(t=>t()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=t=>{this._pointerDownEventTarget=We(t)};_clickListener=t=>{let i=We(t),r=t.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,t,c))){if(z0(a.overlayElement,i)||z0(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(t)):c.next(t)}}};static \u0275fac=(()=>{let t;return function(r){return(t||(t=kt(e)))(r||e)}})();static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function z0(e,n){let t=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===e)return!0;i=t&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var tC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return e})(),nC=(()=>{class e{_platform=u(Ee);_containerElement;_document=u(Y);_styleLoader=u(nt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let t="cdk-overlay-container";if(this._platform.isBrowser||Gh()){let r=this._document.querySelectorAll(`.${t}[platform="server"], .${t}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(t),Gh()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(tC)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Cp=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,t,i,r){this._renderer=t,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=t.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Dp(e){return e&&e.nodeType===1}var Id=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges=ae.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,t,i,r,o,s,a,c,l,d=!1,f,m){this._portalOutlet=n,this._host=t,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=m,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let t=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Tt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof t?.onDestroy=="function"&&t.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),t}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=_(_({},this._config),n),this._updateElementSize()}setDirection(n){this._config=R(_({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Te(this._config.width),n.height=Te(this._config.height),n.minWidth=Te(this._config.minWidth),n.minHeight=Te(this._config.minHeight),n.maxWidth=Te(this._config.maxWidth),n.maxHeight=Te(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Dp(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Cp(this._document,this._renderer,this._ngZone,t=>{this._backdropClick.next(t)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,t,i){let r=Hh(t||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Tt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(t){if(n)throw t;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},$0="cdk-overlay-connected-position-bounding-box",_A=/([A-Za-z%]+)$/;function xp(e,n){return new Sd(n,e.get(Ii),e.get(Y),e.get(Ee),e.get(nC))}var Sd=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=ae.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,t,i,r,o){this._viewportRuler=t,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add($0),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,t=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,t,a),d=this._getOverlayFit(l,t,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:t,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:t})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&pr(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove($0),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof $?this._origin.nativeElement:Dp(this._origin)?this._origin:null}_getOriginPoint(n,t,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}t.left<0&&(r-=t.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,t.top<0&&(o-=t.top),{x:r,y:o}}_getOverlayPoint(n,t,i){let r;i.overlayX=="center"?r=-t.width/2:i.overlayX==="start"?r=this._isRtl()?-t.width:0:r=this._isRtl()?0:-t.width;let o;return i.overlayY=="center"?o=-t.height/2:o=i.overlayY=="top"?0:-t.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,t,i,r){let o=W0(t),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,m=0-a,h=a+o.height-i.height,g=this._subtractOverflows(o.width,d,f),C=this._subtractOverflows(o.height,m,h),W=g*C;return{visibleArea:W,isCompletelyWithinViewport:o.width*o.height===W,fitsInViewportVertically:C===o.height,fitsInViewportHorizontally:g==o.width}}_canFitWithFlexibleDimensions(n,t,i){if(this._hasFlexibleDimensions){let r=i.bottom-t.y,o=i.right-t.x,s=G0(this._overlayRef.getConfig().minHeight),a=G0(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,t,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=W0(t),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,t){if(this._setTransformOrigin(n),this._setOverlayElementStyles(t,n),this._setBoundingBoxStyles(t,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!yA(this._lastScrollVisibility,i)){let r=new Ed(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let t=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<t.length;o++)t[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,t){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(t.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(t.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let h=Math.min(i.bottom-n.y+i.top,n.y),g=this._lastBoundingBoxSize.height;o=h*2,s=n.y-h,o>g&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-g/2)}let c=t.overlayX==="start"&&!r||t.overlayX==="end"&&r,l=t.overlayX==="end"&&!r||t.overlayX==="start"&&r,d,f,m;if(l)m=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let h=Math.min(i.right-n.x+i.left,n.x),g=this._lastBoundingBoxSize.width;d=h*2,f=n.x-h,d>g&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-g/2)}return{top:s,left:f,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(n,t){let i=this._calculateBoundingBoxRect(n,t);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Te(i.width),r.height=Te(i.height),r.top=Te(i.top)||"auto",r.bottom=Te(i.bottom)||"auto",r.left=Te(i.left)||"auto",r.right=Te(i.right)||"auto",t.overlayX==="center"?r.alignItems="center":r.alignItems=t.overlayX==="end"?"flex-end":"flex-start",t.overlayY==="center"?r.justifyContent="center":r.justifyContent=t.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Te(o)),s&&(r.maxWidth=Te(s))}this._lastBoundingBoxSize=i,pr(this._boundingBox.style,r)}_resetBoundingBoxStyles(){pr(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){pr(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,t){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();pr(i,this._getExactOverlayY(t,n,d)),pr(i,this._getExactOverlayX(t,n,d))}else i.position="static";let a="",c=this._getOffset(t,"x"),l=this._getOffset(t,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Te(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Te(s.maxWidth):o&&(i.maxWidth="")),pr(this._pane.style,i)}_getExactOverlayY(n,t,i){let r={top:"",bottom:""},o=this._getOverlayPoint(t,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Te(o.y);return r}_getExactOverlayX(n,t,i){let r={left:"",right:""},o=this._getOverlayPoint(t,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Te(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),t=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:U0(n,i),isOriginOutsideView:wp(n,i),isOverlayClipped:U0(t,i),isOverlayOutsideView:wp(t,i)}}_subtractOverflows(n,...t){return t.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,t=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+t-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:t-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,t){return t==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Hh(n).forEach(t=>{t!==""&&this._appliedPanelClasses.indexOf(t)===-1&&(this._appliedPanelClasses.push(t),this._pane.classList.add(t))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof $)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let t=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+t,height:i,width:t}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",t=this._overlayContainer.getContainerElement();n&&(t.style.display="block");let i=t.getBoundingClientRect();return n&&(t.style.display=""),i}};function pr(e,n){for(let t in n)n.hasOwnProperty(t)&&(e[t]=n[t]);return e}function G0(e){if(typeof e!="number"&&e!=null){let[n,t]=e.split(_A);return!t||t==="px"?parseFloat(n):null}return e||null}function W0(e){return{top:Math.floor(e.top),right:Math.floor(e.right),bottom:Math.floor(e.bottom),left:Math.floor(e.left),width:Math.floor(e.width),height:Math.floor(e.height)}}function yA(e,n){return e===n?!0:e.isOriginClipped===n.isOriginClipped&&e.isOriginOutsideView===n.isOriginOutsideView&&e.isOverlayClipped===n.isOverlayClipped&&e.isOverlayOutsideView===n.isOverlayOutsideView}var q0="cdk-global-overlay-wrapper";function iC(e){return new Md}var Md=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let t=n.getConfig();this._overlayRef=n,this._width&&!t.width&&n.updateSize({width:this._width}),this._height&&!t.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(q0),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",h="",g="",C="";c?C="flex-start":d==="center"?(C="center",m?g=f:h=f):m?d==="left"||d==="end"?(C="flex-end",h=f):(d==="right"||d==="start")&&(C="flex-start",g=f):d==="left"||d==="start"?(C="flex-start",h=f):(d==="right"||d==="end")&&(C="flex-end",g=f),n.position=this._cssPosition,n.marginLeft=c?"0":h,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":g,t.justifyContent=C,t.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement,i=t.style;t.classList.remove(q0),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},rC=(()=>{class e{_injector=u(J);constructor(){}global(){return iC()}flexibleConnectedTo(t){return xp(this._injector,t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),da=new y("OVERLAY_DEFAULT_CONFIG");function Ep(e,n){e.get(nt).load(tC);let t=e.get(nC),i=e.get(Y),r=e.get(ze),o=e.get(yt),s=e.get(Wn),a=e.get(Fe,null,{optional:!0})||e.get(Ne).createRenderer(null,null),c=new ca(n),l=e.get(da,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Dp(m)?m.after(f):m?.type==="parent"?m.element.appendChild(f):t.getContainerElement().appendChild(f),new Id(new wd(d,o,e),f,d,c,e.get(T),e.get(J0),i,e.get($n),e.get(eC),n?.disableAnimations??e.get(as,null,{optional:!0})==="NoopAnimations",e.get(ye),a)}var oC=(()=>{class e{scrollStrategies=u(Z0);_positionBuilder=u(rC);_injector=u(J);constructor(){}create(t){return Ep(this._injector,t)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),bA=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],wA=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let e=u(J);return()=>la(e)}}),wo=(()=>{class e{elementRef=u($);constructor(){}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return e})(),sC=new y("cdk-connected-overlay-default-config"),kd=(()=>{class e{_dir=u(Wn,{optional:!0});_injector=u(J);_overlayRef;_templatePortal;_backdropSubscription=ae.EMPTY;_attachSubscription=ae.EMPTY;_detachSubscription=ae.EMPTY;_positionSubscription=ae.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(wA);_ngZone=u(T);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(t){this._offsetX=t,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(t){this._offsetY=t,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(t){typeof t!="string"&&this._assignConfig(t)}backdropClick=new j;positionChange=new j;attach=new j;detach=new j;overlayKeydown=new j;overlayOutsideClick=new j;constructor(){let t=u(dn),i=u(Rt),r=u(sC,{optional:!0}),o=u(da,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new sa(t,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(t){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),t.origin&&this.open&&this._position.apply()),t.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=bA);let t=this._overlayRef=Ep(this._injector,this._buildConfig());this._attachSubscription=t.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=t.detachments().subscribe(()=>this.detach.emit()),t.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!wn(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=We(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let t=this._position=this.positionStrategy||this._createPositionStrategy(),i=new ca({direction:this._dir||"ltr",positionStrategy:t,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(t){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return t.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let t=xp(this._injector,this._getOrigin());return this._updatePositionStrategy(t),t}_getOrigin(){return this.origin instanceof wo?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof wo?this.origin.elementRef.nativeElement:this.origin instanceof $?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let t=this._overlayRef;t.getConfig().hasBackdrop=this.hasBackdrop,t.updateSize({width:this._getWidth()}),t.hasAttached()||t.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=t.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(fu(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(t){this.origin=t.origin??this.origin,this.positions=t.positions??this.positions,this.positionStrategy=t.positionStrategy??this.positionStrategy,this.offsetX=t.offsetX??this.offsetX,this.offsetY=t.offsetY??this.offsetY,this.width=t.width??this.width,this.height=t.height??this.height,this.minWidth=t.minWidth??this.minWidth,this.minHeight=t.minHeight??this.minHeight,this.backdropClass=t.backdropClass??this.backdropClass,this.panelClass=t.panelClass??this.panelClass,this.viewportMargin=t.viewportMargin??this.viewportMargin,this.scrollStrategy=t.scrollStrategy??this.scrollStrategy,this.disableClose=t.disableClose??this.disableClose,this.transformOriginSelector=t.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=t.hasBackdrop??this.hasBackdrop,this.lockPosition=t.lockPosition??this.lockPosition,this.flexibleDimensions=t.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=t.growAfterOpen??this.growAfterOpen,this.push=t.push??this.push,this.disposeOnNavigation=t.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=t.usePopover??this.usePopover,this.matchWidth=t.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||e)};static \u0275dir=G({type:e,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",P],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",P],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",P],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",P],push:[2,"cdkConnectedOverlayPush","push",P],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",P],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",P],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Oe]})}return e})(),Ip=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({providers:[oC],imports:[pe,B0,ra,ra]})}return e})();var aC=(()=>{class e{_animationsDisabled=it();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&B("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return e})();var CA=["text"],DA=[[["mat-icon"]],"*"],xA=["mat-icon","*"];function EA(e,n){if(e&1&&le(0,"mat-pseudo-checkbox",1),e&2){let t=ie();N("disabled",t.disabled)("state",t.selected?"checked":"unchecked")}}function IA(e,n){if(e&1&&le(0,"mat-pseudo-checkbox",3),e&2){let t=ie();N("disabled",t.disabled)}}function SA(e,n){if(e&1&&(p(0,"span",4),D(1),v()),e&2){let t=ie();b(),_e("(",t.group.label,")")}}var Mp=new y("MAT_OPTION_PARENT_COMPONENT"),kp=new y("MatOptgroup");var Sp=class{source;isUserInput;constructor(n,t=!1){this.source=n,this.isUserInput=t}},Do=(()=>{class e{_element=u($);_changeDetectorRef=u(He);_parent=u(Mp,{optional:!0});group=u(kp,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(ze).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(t){this._disabled.set(t)}_disabled=O(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new j;_text;_stateChanges=new E;constructor(){let t=u(nt);t.load(Ci),t.load(Yh),this._signalDisableRipple=!!this._parent&&Bn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(t=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}deselect(t=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}focus(t,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!wn(t)&&(this._selectViaInteraction(),t.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let t=this.viewValue;t!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=t)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(t=!1){this.onSelectionChange.emit(new Sp(this,t))}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&et(CA,7),i&2){let o;re(o=oe())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&k("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Zt("id",r.id),ge("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),B("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",P]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:xA,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(De(DA),me(0,EA,1,2,"mat-pseudo-checkbox",1),te(1),p(2,"span",2,0),te(4,1),v(),me(5,IA,1,1,"mat-pseudo-checkbox",3),me(6,SA,2,1,"span",4),le(7,"div",5)),i&2&&(he(r.multiple?0:-1),b(5),he(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),b(),he(r.group&&r.group._inert?6:-1),b(),N("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[aC,yo],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return e})();function cC(e,n,t){if(t.length){let i=n.toArray(),r=t.toArray(),o=0;for(let s=0;s<e+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function lC(e,n,t,i){return e<t?e:e+n>t+i?Math.max(0,e-i+n):t}var dC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[pe]})}return e})();var Tp=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[Di,dC,Do,pe]})}return e})();var MA=["trigger"],kA=["panel"],TA=[[["mat-select-trigger"]],"*"],RA=["mat-select-trigger","*"];function AA(e,n){if(e&1&&(p(0,"span",4),D(1),v()),e&2){let t=ie();b(),Hn(t.placeholder)}}function NA(e,n){e&1&&te(0)}function OA(e,n){if(e&1&&(p(0,"span",11),D(1),v()),e&2){let t=ie(2);b(),Hn(t.triggerValue)}}function FA(e,n){if(e&1&&(p(0,"span",5),me(1,NA,1,0)(2,OA,2,1,"span",11),v()),e&2){let t=ie();b(),he(t.customTrigger?1:2)}}function PA(e,n){if(e&1){let t=Ot();p(0,"div",12,1),k("keydown",function(r){gt(t);let o=ie();return vt(o._handleKeydown(r))}),te(2,1),v()}if(e&2){let t=ie();di(t.panelClass),B("mat-select-panel-animations-enabled",!t._animationsDisabled)("mat-primary",(t._parentFormField==null?null:t._parentFormField.color)==="primary")("mat-accent",(t._parentFormField==null?null:t._parentFormField.color)==="accent")("mat-warn",(t._parentFormField==null?null:t._parentFormField.color)==="warn")("mat-undefined",!(t._parentFormField!=null&&t._parentFormField.color)),ge("id",t.id+"-panel")("aria-multiselectable",t.multiple)("aria-label",t.ariaLabel||null)("aria-labelledby",t._getPanelAriaLabelledby())}}var LA=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let e=u(J);return()=>la(e)}}),VA=new y("MAT_SELECT_CONFIG"),jA=new y("MatSelectTrigger"),Rp=class{source;value;constructor(n,t){this.source=n,this.value=t}},uC=(()=>{class e{_viewportRuler=u(Ii);_changeDetectorRef=u(He);_elementRef=u($);_dir=u(Wn,{optional:!0});_idGenerator=u(ze);_renderer=u(Fe);_parentFormField=u(Js,{optional:!0});ngControl=u(po,{self:!0,optional:!0});_liveAnnouncer=u(Kh);_defaultOptions=u(VA,{optional:!0});_animationsDisabled=it();_popoverLocation;_initialized=new E;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(t){let i=this.options.toArray()[t];if(i){let r=this.panel.nativeElement,o=cC(t,this.options,this.optionGroups),s=i._getHostElement();t===0&&o===1?r.scrollTop=0:r.scrollTop=lC(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(t){return new Rp(this,t)}_scrollStrategyFactory=u(LA);_panelOpen=!1;_compareWith=(t,i)=>t===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new E;_errorStateTracker;stateChanges=new E;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(t){this._disableRipple.set(t)}_disableRipple=O(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(t){this._hideSingleSelectionIndicator=t,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(t){this._placeholder=t,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(mo.required)??!1}set required(t){this._required=t,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(t){this._selectionModel,this._multiple=t}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(t){this._compareWith=t,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(t){this._assignValue(t)&&this._onChange(t)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(t){this._errorStateTracker.matcher=t}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(t){this._id=t||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(t){this._errorStateTracker.errorState=t}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Pi(()=>{let t=this.options;return t?t.changes.pipe(In(t),ot(()=>Li(...t.map(i=>i.onSelectionChange)))):this._initialized.pipe(ot(()=>this.optionSelectionChanges))});openedChange=new j;_openedStream=this.openedChange.pipe(we(t=>t),ce(()=>{}));_closedStream=this.openedChange.pipe(we(t=>!t),ce(()=>{}));selectionChange=new j;valueChange=new j;constructor(){let t=u(go),i=u(Ks,{optional:!0}),r=u(Zs,{optional:!0}),o=u(new Un("tabindex"),{optional:!0}),s=u(da,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new vo(t,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new ea(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe($e(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe($e(this._destroy)).subscribe(t=>{t.added.forEach(i=>i.select()),t.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(In(null),$e(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let t=this._getTriggerAriaLabelledby(),i=this.ngControl;if(t!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=t,t?r.setAttribute("aria-labelledby",t):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(t){(t.disabled||t.userAriaDescribedBy)&&this.stateChanges.next(),t.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),t.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ht(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let t=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!t)return;let i=`${this.id}-panel`;this._trackedModal&&ep(this._trackedModal,"aria-owns",i),zw(t,"aria-owns",i),this._trackedModal=t}_clearFromModal(){if(!this._trackedModal)return;let t=`${this.id}-panel`;ep(this._trackedModal,"aria-owns",t),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let t=this.panel.nativeElement,i=this._renderer.listen(t,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);t.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(t){this._assignValue(t)}registerOnChange(t){this._onChange=t}registerOnTouched(t){this._onTouched=t}setDisabledState(t){this.disabled=t,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let t=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&t.reverse(),t.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(t){this.disabled||(this.panelOpen?this._handleOpenKeydown(t):this._handleClosedKeydown(t))}_handleClosedKeydown(t){let i=t.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!wn(t)||(this.multiple||t.altKey)&&r)t.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(t);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(t){let i=this._keyManager,r=t.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&t.altKey)t.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!wn(t))t.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&t.ctrlKey){t.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(t),this._multiple&&o&&t.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(t){t.keyCode===27&&!wn(t)&&(t.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(t){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&t)Array.isArray(t),t.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(t);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(t){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,t)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(t){return t!==this._value||this._multiple&&Array.isArray(t)?(this.options&&this._setSelectionByValue(t),this._value=t,!0):!1}_skipPredicate=t=>this.panelOpen?!1:t.disabled;_getOverlayWidth(t){return this.panelWidth==="auto"?(t instanceof wo?t.elementRef:t||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let t of this.options)t._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new $s(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let t=Li(this.options.changes,this._destroy);this.optionSelectionChanges.pipe($e(t)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Li(...this.options.map(i=>i._stateChanges)).pipe($e(t)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(t,i){let r=this._selectionModel.isSelected(t);!this.canSelectNullableOptions&&t.value==null&&!this._multiple?(t.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(t.value)):(r!==t.selected&&(t.selected?this._selectionModel.select(t):this._selectionModel.deselect(t)),i&&this._keyManager.setActiveItem(t),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(t)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let t=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,t):t.indexOf(i)-t.indexOf(r)),this.stateChanges.next()}}_propagateChanges(t){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:t,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let t=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){t=i;break}this._keyManager.setActiveItem(t)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(t){this._elementRef.nativeElement.focus(t)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let t=this._parentFormField?.getLabelId()||null,i=t?t+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:t}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let t=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(t+=" "+this.ariaLabelledby),t||(t=this._valueId),t}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(t){let i=this._elementRef.nativeElement;t.length?i.setAttribute("aria-describedby",t.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(t){let i=We(t);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Xt(o,jA,5)(o,Do,5)(o,kp,5),i&2){let s;re(s=oe())&&(r.customTrigger=s.first),re(s=oe())&&(r.options=s),re(s=oe())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&et(MA,5)(kA,5)(kd,5),i&2){let o;re(o=oe())&&(r.trigger=o.first),re(o=oe())&&(r.panel=o.first),re(o=oe())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&k("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ge("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),B("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",P],disableRipple:[2,"disableRipple","disableRipple",P],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Ft(t)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",P],placeholder:"placeholder",required:[2,"required","required",P],multiple:[2,"multiple","multiple",P],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",P],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Ft],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",P]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Pe([{provide:Xs,useExisting:e},{provide:Mp,useExisting:e}]),Oe],ngContentSelectors:RA,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(De(TA),p(0,"div",2,0),k("click",function(){return r.open()}),p(3,"div",3),me(4,AA,2,1,"span",4)(5,FA,3,1,"span",5),v(),p(6,"div",6)(7,"div",7),Ji(),p(8,"svg",8),le(9,"path",9),v()()()(),mn(10,PA,3,16,"ng-template",10),k("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=li(1);b(3),ge("id",r._valueId),b(),he(r.empty?4:5),b(6),N("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[wo,kd],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return e})();var xo=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[Ip,Tp,pe,bd,en,Tp]})}return e})();var HA=["switch"],UA=["*"];function zA(e,n){e&1&&(p(0,"span",11),Ji(),p(1,"svg",13),le(2,"path",14),v(),p(3,"svg",15),le(4,"path",16),v()())}var $A=new y("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),Rd=class{source;checked;constructor(n,t){this.source=n,this.checked=t}},fC=(()=>{class e{_elementRef=u($);_focusMonitor=u(mr);_changeDetectorRef=u(He);defaults=u($A);_onChange=t=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(t){return new Rd(this,t)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=it();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(t){this._checked=t,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new j;toggleChange=new j;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){u(nt).load(Ci);let t=u(new Un("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=t==null?0:parseInt(t)||0,this.color=i.color||"accent",this.id=this._uniqueId=u(ze).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{t==="keyboard"||t==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):t||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(t){t.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(t){this.checked=!!t}registerOnChange(t){this._onChange=t}registerOnTouched(t){this._onTouched=t}validate(t){return this.required&&t.value!==!0?{required:!0}:null}registerOnValidatorChange(t){this._validatorOnChange=t}setDisabledState(t){this.disabled=t,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new Rd(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&et(HA,5),i&2){let o;re(o=oe())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Zt("id",r.id),ge("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),di(r.color?"mat-"+r.color:""),B("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",P],color:"color",disabled:[2,"disabled","disabled",P],disableRipple:[2,"disableRipple","disableRipple",P],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Ft(t)],checked:[2,"checked","checked",P],hideIcon:[2,"hideIcon","hideIcon",P],disabledInteractive:[2,"disabledInteractive","disabledInteractive",P]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[Pe([{provide:fd,useExisting:ht(()=>e),multi:!0},{provide:md,useExisting:e,multi:!0}]),Oe],ngContentSelectors:UA,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(De(),p(0,"div",1)(1,"button",2,0),k("click",function(){return r._handleClick()}),le(3,"div",3)(4,"span",4),p(5,"span",5)(6,"span",6)(7,"span",7),le(8,"span",8),v(),p(9,"span",9),le(10,"span",10),v(),me(11,zA,5,0,"span",11),v()()(),p(12,"label",12),k("click",function(s){return s.stopPropagation()}),te(13),v()()),i&2){let o=li(2);N("labelPosition",r.labelPosition),b(),B("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),N("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),ge("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),b(9),N("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),b(),he(r.hideIcon?-1:11),b(),N("for",r.buttonId),ge("id",r._labelId)}},dependencies:[yo,vd],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return e})();var Ad=class e{koiKoiBonus=bt.required();flowerViewingSake=bt.required();moonViewingSake=bt.required();sevenPointCap=bt.required();koiKoiBonusChange=pn();flowerViewingSakeChange=pn();moonViewingSakeChange=pn();sevenPointCapChange=pn();static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-ruleset-options"]],inputs:{koiKoiBonus:[1,"koiKoiBonus"],flowerViewingSake:[1,"flowerViewingSake"],moonViewingSake:[1,"moonViewingSake"],sevenPointCap:[1,"sevenPointCap"]},outputs:{koiKoiBonusChange:"koiKoiBonusChange",flowerViewingSakeChange:"flowerViewingSakeChange",moonViewingSakeChange:"moonViewingSakeChange",sevenPointCapChange:"sevenPointCapChange"},decls:12,vars:4,consts:[[1,"setup-section"],[1,"rules-list"],[3,"change","checked"]],template:function(t,i){t&1&&(p(0,"section",0)(1,"h2"),D(2,"Rules"),v(),p(3,"div",1)(4,"mat-slide-toggle",2),k("change",function(o){return i.koiKoiBonusChange.emit(o.checked)}),D(5," Koi-Koi Bonus "),v(),p(6,"mat-slide-toggle",2),k("change",function(o){return i.flowerViewingSakeChange.emit(o.checked)}),D(7," Flower Viewing Sake "),v(),p(8,"mat-slide-toggle",2),k("change",function(o){return i.moonViewingSakeChange.emit(o.checked)}),D(9," Moon Viewing Sake "),v(),p(10,"mat-slide-toggle",2),k("change",function(o){return i.sevenPointCapChange.emit(o.checked)}),D(11," Seven Point Cap "),v()()()),t&2&&(b(4),N("checked",i.koiKoiBonus()),b(2),N("checked",i.flowerViewingSake()),b(2),N("checked",i.moonViewingSake()),b(2),N("checked",i.sevenPointCap()))},dependencies:[bn,wi,xi,Dn,xo,fC],styles:[".rules-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}"]})};var GA=(e,n)=>n.id;function WA(e,n){if(e&1){let t=Ot();p(0,"mat-form-field",2)(1,"mat-label"),D(2),v(),p(3,"input",3),k("input",function(r){let o=gt(t).$implicit,s=ie();return vt(s.updateValue(o.id,+r.target.value))}),v()()}if(e&2){let t=n.$implicit,i=ie();b(2),_e(" ",t.name," "),b(),N("value",i.Math.abs(i.values()[t.id]))}}var Nd=class e{yakus=bt.required();values=bt.required();valuesChanged=pn();updateValue(n,t){this.valuesChanged.emit(R(_({},this.values()),{[n]:t}))}Math=Math;static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-yaku-values-editor"]],inputs:{yakus:[1,"yakus"],values:[1,"values"]},outputs:{valuesChanged:"valuesChanged"},decls:6,vars:0,consts:[[1,"setup-section"],[1,"yaku-grid"],["appearance","outline",1,"full-width"],["matInput","","type","number","min","1","step","1",3,"input","value"]],template:function(t,i){t&1&&(p(0,"section",0)(1,"h2"),D(2,"Custom Yaku Values"),v(),p(3,"div",1),Xe(4,WA,4,2,"mat-form-field",2,GA),v()()),t&2&&(b(4),Je(i.yakus()))},dependencies:[bn,wi,_o,Yn,Cn,xi,Dn,xo,en],styles:[".yaku-grid[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}.full-width[_ngcontent-%COMP%]{width:100%}"]})};var Si=class e{players=O([]);phase=O("Setup");currentRound=O(1);ruleset=O(null);roundHistory=O([]);oyaPlayerId=O(null);totalRounds=xe(()=>this.ruleset()?.rounds??0);gameFinished=xe(()=>this.currentRound()>=this.totalRounds());startGame(n,t,i){this.players.set(n),this.ruleset.set(t),this.oyaPlayerId.set(i),this.currentRound.set(1),this.phase.set("Round")}applyRound(n){this.roundHistory.update(t=>[...t,n]),this.players.update(t=>t.map(i=>i.id!==n.winnerId?i:R(_({},i),{score:i.score+n.totalPoints}))),this.currentRound.update(t=>t+1),this.gameFinished()&&this.phase.set("Finished")}resetGame(){this.players.set([]),this.roundHistory.set([]),this.phase.set("Setup"),this.currentRound.set(1),this.ruleset.set(null),this.oyaPlayerId.set(null)}static \u0275fac=function(t){return new(t||e)};static \u0275prov=w({token:e,factory:e.\u0275fac,providedIn:"root"})};var mC=[{id:"standard",name:"Standard",values:{plants:1,"monthly-card":6,ribbons:1,"poetry-ribbons":6,"blue-ribbons":6,animals:1,"boar-deer-butterfly":6,"three-brights":6,"four-brights":10,"four-rainy-brights":8,"five-brights":15,"moon-viewing-sake":5,"flower-viewing-sake":5}},{id:"iello",name:"IELLO",values:{plants:7,"monthly-card":6,ribbons:1,"poetry-ribbons":6,"blue-ribbons":6,animals:1,"boar-deer-butterfly":6,"three-brights":6,"four-brights":10,"four-rainy-brights":8,"five-brights":15,"moon-viewing-sake":5,"flower-viewing-sake":5}}];var Od=[{id:"plants",name:"Plants"},{id:"monthly-card",name:"Monthly Card"},{id:"ribbons",name:"Ribbon"},{id:"poetry-ribbons",name:"Poetry Ribbons"},{id:"blue-ribbons",name:"Blue ribbons"},{id:"animals",name:"Animals"},{id:"boar-deer-butterfly",name:"Boar Deer Butterfly"},{id:"three-brights",name:"Three Brights"},{id:"four-brights",name:"Four Brights"},{id:"four-rainy-brights",name:"Four Rainy Brights"},{id:"five-brights",name:"Five Brights"},{id:"moon-viewing-sake",name:"Moon Viewing"},{id:"flower-viewing-sake",name:"Flower Viewing"}];var YA=(e,n)=>n.id;function QA(e,n){if(e&1){let t=Ot();p(0,"mat-form-field",3)(1,"mat-label"),D(2),v(),p(3,"input",11),k("input",function(r){let o=gt(t).$index,s=ie();return vt(s.updatePlayerName(r.target.value,o))}),v()()}if(e&2){let t=n.$implicit,i=n.$index;b(2),_e(" Player ",i+1," "),b(),N("value",t)}}function KA(e,n){if(e&1&&(p(0,"mat-option",8),D(1),v()),e&2){let t=n.$implicit,i=n.$index;N("value",i),b(),_e(" ",t||"Player "+(i+1)," ")}}function ZA(e,n){if(e&1&&(p(0,"mat-option",8),D(1),v()),e&2){let t=n.$implicit;N("value",t.id),b(),_e(" ",t.name," ")}}var Fd=class e{gameStore=u(Si);router=u(yn);playerNames=O(["",""]);rounds=O(12);koiKoiBonus=O(!0);flowerViewingSake=O(!0);moonViewingSake=O(!0);sevenPointCap=O(!1);oyaIndex=O(0);selectedPresetId=O("standard");customYakuValues=O({});presets=mC;yakus=Od;validSetup=xe(()=>{let n=this.playerNames().every(i=>i.trim().length>0),t=Object.values(this.customYakuValues()).every(i=>Number.isInteger(i)&&i>0);return n&&t});constructor(){this.selectPreset("standard")}startGame(){let n=this.playerNames().map(i=>({id:crypto.randomUUID(),name:i,score:0})),t={rounds:this.rounds(),koiKoiBonus:this.koiKoiBonus(),flowerViewingSake:this.flowerViewingSake(),moonViewingSake:this.moonViewingSake(),sevenPointCap:this.sevenPointCap(),yakuValues:this.customYakuValues()};this.gameStore.startGame(n,t,n[this.oyaIndex()].id),this.router.navigate(["/game"])}updatePlayerName(n,t){this.playerNames.update(i=>{let r=[...i];return r[t]=n,r})}selectPreset(n){this.selectedPresetId.set(n);let t=this.presets.find(i=>i.id===n);t&&this.customYakuValues.set(_({},t.values))}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-setup-page"]],decls:41,vars:12,consts:[[1,"setup-page"],[1,"setup-card"],[1,"setup-section"],["appearance","outline",1,"full-width"],[1,"rounds-group",3,"value"],[3,"change","value"],[3,"koiKoiBonusChange","flowerViewingSakeChange","moonViewingSakeChange","sevenPointCapChange","koiKoiBonus","flowerViewingSake","moonViewingSake","sevenPointCap"],[3,"selectionChange","value"],[3,"value"],[3,"valuesChanged","yakus","values"],["mat-flat-button","","color","primary",1,"start-button",3,"click","disabled"],["matInput","",3,"input","value"]],template:function(t,i){t&1&&(p(0,"div",0)(1,"mat-card",1)(2,"mat-card-header")(3,"mat-card-title"),D(4," Koi-Koi Counter "),v(),p(5,"mat-card-subtitle"),D(6," Game Setup "),v()(),p(7,"mat-card-content")(8,"section",2)(9,"h2"),D(10,"Players"),v(),Xe(11,QA,4,2,"mat-form-field",3,Gr),v(),p(13,"section",2)(14,"h2"),D(15,"Rounds"),v(),p(16,"mat-radio-group",4)(17,"mat-radio-button",5),k("change",function(){return i.rounds.set(6)}),D(18," 6 Rounds "),v(),p(19,"mat-radio-button",5),k("change",function(){return i.rounds.set(12)}),D(20," 12 Rounds "),v()()(),p(21,"app-ruleset-options",6),k("koiKoiBonusChange",function(o){return i.koiKoiBonus.set(o)})("flowerViewingSakeChange",function(o){return i.flowerViewingSake.set(o)})("moonViewingSakeChange",function(o){return i.moonViewingSake.set(o)})("sevenPointCapChange",function(o){return i.sevenPointCap.set(o)}),v(),p(22,"section",2)(23,"h2"),D(24,"Starting Oya"),v(),p(25,"mat-form-field",3)(26,"mat-label"),D(27," Oya "),v(),p(28,"mat-select",7),k("selectionChange",function(o){return i.oyaIndex.set(o.value)}),Xe(29,KA,2,2,"mat-option",8,Gr),v()()(),p(31,"section",2)(32,"h2"),D(33,"Score Preset"),v(),p(34,"mat-select",5),k("change",function(o){return i.selectPreset(o.target.value)}),Xe(35,ZA,2,2,"mat-option",8,YA),v()(),p(37,"app-yaku-values-editor",9),k("valuesChanged",function(o){return i.customYakuValues.set(o)}),v()(),p(38,"mat-card-actions")(39,"button",10),k("click",function(){return i.startGame()}),D(40," Start game "),v()()()()),t&2&&(b(11),Je(i.playerNames()),b(5),N("value",i.rounds()),b(),N("value",6),b(2),N("value",12),b(2),N("koiKoiBonus",i.koiKoiBonus())("flowerViewingSake",i.flowerViewingSake())("moonViewingSake",i.moonViewingSake())("sevenPointCap",i.sevenPointCap()),b(7),N("value",i.oyaIndex()),b(),Je(i.playerNames()),b(5),N("value",i.selectedPresetId()),b(),Je(i.presets),b(2),N("yakus",i.yakus)("values",i.customYakuValues()),b(2),N("disabled",!i.validSetup()))},dependencies:[bn,gi,ww,_i,yi,ao,vi,wi,_o,Yn,Cn,xi,ia,bo,Dn,Ei,xo,uC,Do,en,Ad,Nd],styles:[".setup-page[_ngcontent-%COMP%]{padding:1rem;display:flex;justify-content:center}.setup-card[_ngcontent-%COMP%]{width:100%;max-width:600px}.setup-section[_ngcontent-%COMP%]{margin-bottom:2rem}.setup-section[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin-bottom:1rem;font-size:1.1rem;font-weight:600}.full-width[_ngcontent-%COMP%]{width:100%}.rounds-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}.start-button[_ngcontent-%COMP%]{width:100%;height:48px;font-size:1rem;font-weight:600}mat-card-actions[_ngcontent-%COMP%]{padding:1rem}"]})};function hC(e,n,t){let i=e.reduce((r,o)=>{let s=t.yakuValues[o.id]??0;return r+s},0);return t.koiKoiBonus&&(i*=n+1),t.sevenPointCap&&i>=7&&(i=7),i}var XA=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})});var pC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({providers:[go,{provide:XA,useValue:{separatorKeyCodes:[13]}}],imports:[Di,pe]})}return e})();var ua=(()=>{class e{get vertical(){return this._vertical}set vertical(t){this._vertical=qn(t)}_vertical=!1;get inset(){return this._inset}set inset(t){this._inset=qn(t)}_inset=!1;static \u0275fac=function(i){return new(i||e)};static \u0275cmp=M({type:e,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(ge("aria-orientation",r.vertical?"vertical":"horizontal"),B("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return e})(),Pd=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[pe]})}return e})();var eN=(e,n)=>n.id;function tN(e,n){if(e&1){let t=Ot();p(0,"button",7),k("click",function(){let r=gt(t).$implicit,o=ie();return vt(o.yakuSelected.emit(r.id))}),D(1),v()}if(e&2){let t=n.$implicit;b(),_e(" ",t.name," ")}}function nN(e,n){e&1&&(p(0,"p",4),D(1," No yakus selected "),v())}function iN(e,n){if(e&1){let t=Ot();p(0,"button",8),k("click",function(){let r=gt(t).$implicit,o=ie();return vt(o.yakuDeselected.emit(r.id))}),D(1),v()}if(e&2){let t=n.$implicit;b(),_e(" ",t.name," \u2715 ")}}var Ld=class e{yakus=bt.required();selectedYakus=bt.required();yakuSelected=pn();yakuDeselected=pn();static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-yaku-selector"]],inputs:{yakus:[1,"yakus"],selectedYakus:[1,"selectedYakus"]},outputs:{yakuSelected:"yakuSelected",yakuDeselected:"yakuDeselected"},decls:16,vars:1,consts:[[1,"round-card"],[1,"yaku-selection-grid"],["mat-stroked-button","",1,"yaku-select-button"],[1,"selected-yaku-section"],[1,"empty-text"],[1,"selected-yaku-list"],["mat-flat-button","","color","primary",1,"selected-yaku-button"],["mat-stroked-button","",1,"yaku-select-button",3,"click"],["mat-flat-button","","color","primary",1,"selected-yaku-button",3,"click"]],template:function(t,i){t&1&&(p(0,"mat-card",0)(1,"mat-card-header",0)(2,"mat-card-title"),D(3," Yakus "),v()(),p(4,"mat-card-content")(5,"div",1),Xe(6,tN,2,1,"button",2,eN),v(),le(8,"mat-divider"),p(9,"div",3)(10,"h3"),D(11," Selected Yakus "),v(),me(12,nN,2,0,"p",4),p(13,"div",5),Xe(14,iN,2,1,"button",6,Gr),v()()()()),t&2&&(b(6),Je(i.yakus()),b(6),he(i.selectedYakus().length===0?12:-1),b(2),Je(i.selectedYakus()))},dependencies:[Ei,gi,_i,yi,vi,ua],styles:[".yaku-selection-grid[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.75rem;margin-bottom:1.5rem}.yaku-select-button[_ngcontent-%COMP%]{min-height:44px}.selected-yaku-section[_ngcontent-%COMP%]{margin-top:1.5rem}.selected-yaku-list[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.75rem;margin-top:1rem}.selected-yaku-button[_ngcontent-%COMP%]{min-height:44px}.empty-text[_ngcontent-%COMP%]{opacity:.7;margin-top:1rem}.round-card[_ngcontent-%COMP%]{padding-bottom:1rem}"]})};var gC=(e,n)=>n.id;function rN(e,n){if(e&1&&(p(0,"mat-card",17)(1,"mat-card-content")(2,"div",18)(3,"div")(4,"h3"),D(5),v(),p(6,"p"),D(7,"Score"),v()(),p(8,"div",19),D(9),v()()()()),e&2){let t=n.$implicit,i=ie();B("winner-selected",i.winnerId()===t.id),b(5),Hn(t.name),b(4),_e(" ",t.score," ")}}function oN(e,n){if(e&1){let t=Ot();p(0,"mat-radio-button",20),k("change",function(){let r=gt(t).$implicit,o=ie();return vt(o.winnerId.set(r.id))}),D(1),v()}if(e&2){let t=n.$implicit;N("value",t.id),b(),_e(" ",t.name," ")}}var Vd=class e{gameStore=u(Si);router=u(yn);currentRound=this.gameStore.currentRound;players=this.gameStore.players;ruleset=this.gameStore.ruleset;winnerId=O("");selectedYakus=O([]);koiKoiCount=O(0);yakus=Od;roundValue=xe(()=>{let n=this.ruleset();return n?hC(this.selectedYakus(),this.koiKoiCount(),n):0});validRound=xe(()=>this.players().length===0?!1:this.koiKoiCount()>=0&&this.players().filter(n=>n.id===this.winnerId()).length);constructor(){this.ruleset()?.flowerViewingSake===!1&&(this.yakus=this.yakus.filter(n=>n.id!=="flower-viewing-sake")),this.ruleset()?.moonViewingSake===!1&&(this.yakus=this.yakus.filter(n=>n.id!=="moon-viewing-sake"))}applyRound(){this.gameStore.phase()!=="Round"&&this.router.navigate(["/summary"]);let n={roundNumber:this.gameStore.currentRound(),winnerId:this.winnerId(),yakus:this.selectedYakus(),koiKoiCount:this.koiKoiCount(),stopped:!0,totalPoints:this.roundValue()};this.gameStore.applyRound(n),this.selectedYakus.set([]),this.koiKoiCount.set(0)}resetRound(){this.winnerId.set(""),this.selectedYakus.set([]),this.koiKoiCount.set(0)}selectYaku(n){this.selectedYakus.update(t=>[...t,this.yakus.find(i=>i.id===n)].sort((i,r)=>i.id.localeCompare(r.id)))}deselectYaku(n){this.selectedYakus.update(t=>{let i=t.findIndex(r=>r.id===n);return i===-1?t:[...t.slice(0,i),...t.slice(i+1)]})}Math=Math;static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-round-page"]],decls:41,vars:7,consts:[[1,"round-page"],[1,"header-card"],[1,"vertical-padding"],[1,"scores-section"],[1,"score-card",3,"winner-selected"],[1,"round-card"],[1,"winner-group",3,"value"],[3,"value"],[3,"yakuSelected","yakuDeselected","yakus","selectedYakus"],["appearance","outline",1,"full-width"],["matInput","","type","number","min","0","step","1",3,"input","value"],[1,"value-card"],[1,"value-content"],[1,"value-label"],[1,"actions"],["mat-flat-button","","color","primary",1,"action-button",3,"click","disabled"],["mat-stroked-button","",1,"action-button",3,"click"],[1,"score-card"],[1,"score-card-content"],[1,"score-value"],[3,"change","value"]],template:function(t,i){t&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-card-title"),D(4),v(),p(5,"mat-card-subtitle"),D(6," Koi-Koi Counter "),v()()(),p(7,"section",3),Xe(8,rN,10,4,"mat-card",4,gC),v(),p(10,"mat-card",5)(11,"mat-card-header",5)(12,"mat-card-title"),D(13," Winner "),v()(),p(14,"mat-card-content")(15,"mat-radio-group",6),Xe(16,oN,2,2,"mat-radio-button",7,gC),v()()(),p(18,"app-yaku-selector",8),k("yakuSelected",function(o){return i.selectYaku(o)})("yakuDeselected",function(o){return i.deselectYaku(o)}),v(),p(19,"mat-card",5)(20,"mat-card-header",5)(21,"mat-card-title"),D(22," Koi-Koi Bonus "),v()(),p(23,"mat-card-content")(24,"mat-form-field",9)(25,"mat-label"),D(26," Koi-Koi Count "),v(),p(27,"input",10),k("input",function(o){return i.koiKoiCount.set(i.Math.abs(o.target.valueAsNumber))}),v()()()(),p(28,"mat-card",11)(29,"mat-card-content")(30,"div",12)(31,"div")(32,"p",13),D(33," Round Value "),v(),p(34,"h1"),D(35),v()()()()(),p(36,"div",14)(37,"button",15),k("click",function(){return i.applyRound()}),D(38," Submit Round "),v(),p(39,"button",16),k("click",function(){return i.resetRound()}),D(40," Reset "),v()()()),t&2&&(b(4),_e(" Round ",i.currentRound()," "),b(4),Je(i.players()),b(7),N("value",i.winnerId()),b(),Je(i.players()),b(2),N("yakus",i.yakus)("selectedYakus",i.selectedYakus()),b(9),N("value",i.koiKoiCount()),b(8),_e(" ",i.roundValue()," "),b(2),N("disabled",!i.validRound()))},dependencies:[bn,gi,_i,yi,ao,vi,Dn,Ei,xi,ia,bo,wi,_o,Yn,Cn,en,pC,Pd,Ld],styles:[".round-page[_ngcontent-%COMP%]{padding:1rem;display:flex;flex-direction:column;gap:1rem;max-width:700px;margin:0 auto}.header-card[_ngcontent-%COMP%]{text-align:center}.scores-section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}.score-card[_ngcontent-%COMP%]{transition:transform .2s ease,border-color .2s ease;border:2px solid transparent}.winner-selected[_ngcontent-%COMP%]{border-color:var(--mat-sys-primary);transform:scale(1.02)}.score-card-content[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.score-value[_ngcontent-%COMP%]{font-size:2rem;font-weight:700}.round-card[_ngcontent-%COMP%]{padding-bottom:1rem}.winner-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}.yaku-chip-list[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:.75rem}.yaku-button[_ngcontent-%COMP%]{min-height:44px}.selected-yaku[_ngcontent-%COMP%]{background-color:var(--mat-sys-primary);color:#fff}.full-width[_ngcontent-%COMP%]{width:100%}.value-card[_ngcontent-%COMP%]{text-align:center}.value-content[_ngcontent-%COMP%]{padding:1rem}.value-label[_ngcontent-%COMP%]{margin-bottom:.5rem;opacity:.7}.actions[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem;margin-bottom:2rem}.action-button[_ngcontent-%COMP%]{height:52px;font-size:1rem;font-weight:600}.vertical-padding[_ngcontent-%COMP%]{padding-top:1rem;padding-bottom:1rem}"]})};var vC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[ra]})}return e})();var _C=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=V({type:e});static \u0275inj=F({imports:[vC,pe]})}return e})();var sN=(e,n)=>n.id,aN=(e,n)=>n.roundNumber;function cN(e,n){if(e&1&&(p(0,"mat-card",8)(1,"mat-card-content")(2,"div",12)(3,"div")(4,"h3",13),D(5),v(),p(6,"p",14),D(7," Total Score "),v()(),p(8,"div",15),D(9),v()()()()),e&2){let t=n.$implicit;b(5),_e(" ",t.name," "),b(4),_e(" ",t.score," ")}}function lN(e,n){if(e&1&&(p(0,"div",16)(1,"div",17),D(2),v(),p(3,"div",18),D(4),v()(),le(5,"mat-divider")),e&2){let t=n.$implicit,i=ie();b(2),_e(" Round ",t.roundNumber," "),b(2),pl(" ",i.getWinnerName(t.winnerId)," +",t.totalPoints," ")}}var jd=class e{gameStore=u(Si);router=u(yn);players=this.gameStore.players;roundHistory=this.gameStore.roundHistory;winner=xe(()=>[...this.players()].sort((n,t)=>t.score-n.score)[0]);resetGame(){this.gameStore.resetGame(),this.router.navigate(["/setup"])}getWinnerName(n){return this.players().find(t=>t.id===n)?.name??"Unknown"}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-summary-page"]],decls:28,vars:2,consts:[[1,"summary-page"],[1,"header-card"],[1,"vertical-padding"],[1,"winner-card"],[1,"winner-label"],[1,"winner-name"],[1,"winner-score"],[1,"scores-section"],[1,"score-card"],[1,"history-card"],[1,"history-list"],["mat-flat-button","","color","primary",1,"reset-button",3,"click"],[1,"score-card-content"],[1,"player-name"],[1,"score-label"],[1,"score-value"],[1,"history-item"],[1,"history-round"],[1,"history-result"]],template:function(t,i){t&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-card-title"),D(4," Game Summary "),v(),p(5,"mat-card-subtitle"),D(6," Final Results "),v()()(),p(7,"mat-card",3)(8,"mat-card-title",4),D(9," Winner "),v(),p(10,"mat-card-content")(11,"h1",5),D(12),v(),p(13,"p",6),D(14),v()()(),p(15,"section",7),Xe(16,cN,10,2,"mat-card",8,sN),v(),p(18,"mat-card",9)(19,"mat-card-header")(20,"mat-card-title"),D(21," Round History "),v()(),p(22,"mat-card-content")(23,"div",10),Xe(24,lN,6,3,null,null,aN),v()()(),p(26,"button",11),k("click",function(){return i.resetGame()}),D(27," Return to Setup "),v()()),t&2&&(b(12),_e(" ",i.winner().name," "),b(2),_e(" ",i.winner().score," points "),b(2),Je(i.players()),b(8),Je(i.roundHistory()))},dependencies:[bn,gi,_i,yi,ao,vi,Dn,Ei,Pd,ua,_C],styles:[".summary-page[_ngcontent-%COMP%]{padding:1rem;display:flex;flex-direction:column;gap:1rem;max-width:700px;margin:0 auto}.header-card[_ngcontent-%COMP%]{text-align:center}.winner-card[_ngcontent-%COMP%]{text-align:center;padding:1rem}.winner-label[_ngcontent-%COMP%]{opacity:.7;margin-bottom:.5rem}.winner-name[_ngcontent-%COMP%]{font-size:2rem;margin:0}.winner-score[_ngcontent-%COMP%]{font-size:1.2rem;margin-top:.5rem;font-weight:600}.scores-section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:1rem}.score-card-content[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.score-value[_ngcontent-%COMP%]{font-size:2rem;font-weight:700}.history-card[_ngcontent-%COMP%]{margin-top:1rem}.history-list[_ngcontent-%COMP%]{display:flex;flex-direction:column}.history-item[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;padding:1rem 0}.history-round[_ngcontent-%COMP%]{font-weight:600}.history-result[_ngcontent-%COMP%]{font-size:1rem}.reset-button[_ngcontent-%COMP%]{height:52px;font-size:1rem;font-weight:600;margin-bottom:2rem}.vertical-padding[_ngcontent-%COMP%]{padding-top:1rem;padding-bottom:1rem}"]})};var yC=[{path:"",redirectTo:"setup",pathMatch:"full"},{path:"setup",component:Fd},{path:"game",component:Vd},{path:"summary",component:jd}];var bC={providers:[gf(),Vh(yC,jh({scrollPositionRestoration:"top",anchorScrolling:"enabled"}))]};var Bd=class e{title=O("koi-koi-counter");static \u0275fac=function(t){return new(t||e)};static \u0275cmp=M({type:e,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&le(0,"router-outlet")},dependencies:[Ps],encapsulation:2})};lh(Bd,bC).catch(e=>console.error(e));

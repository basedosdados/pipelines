# RBA licence finding (verified 2026-08-18)

Source: https://www.rba.gov.au/copyright/ — "Copyright and Disclaimer Notice"

## Default: CC BY 4.0

> "With exception of the Excluded Material, all RBA Material is provided under a Creative
> Commons Attribution 4.0 International License (CC BY 4.0 Licence) and may be used in
> accordance with the terms of that licence. The materials covered by this licence may be
> reproduced, published, communicated to the public and adapted provided that the RBA is
> properly attributed."

Required attribution (Section 3): `Source: Reserve Bank of Australia [year]`, or `Source: RBA [year]`.

## Excluded Material — the gate that matters

"Third Party Material" is Excluded Material and is **not** under CC BY:

> "Material containing, or derived from or prepared using, content obtained from a third
> party, whether it has been: reproduced in whole or in part in the form supplied by the
> third party and published as having a third party source; **or** used by the RBA to derive
> or prepare other material published as having **both a RBA and a third party source**, may
> not be reproduced, published, communicated to the public, adapted or otherwise used in
> whole or part without obtaining the consent of the third party."

The second limb is the sharp one: a series labelled `Source = "ABS / RBA"` is still Third
Party Material, despite naming the RBA.

The RBA labels this for us:

> "The RBA has made all reasonable efforts clearly to label material as having a third party
> source when that material contains, has been derived from or prepared using content
> obtained from a third party."

Each statistical-table CSV carries a per-series `Source` row, and that row *is* the label.
The redistribution filter is therefore mechanical and auditable: **keep only series whose
`Source` names the RBA alone; drop every series naming a third party** (ABS, ASX, FENICS,
Markit, Bloomberg, APRA, ...).

This mirrors the `us_fed_fred` precedent, where copyright-restricted series were excluded at
download time rather than filtered later.

## Sections 4 and 5 (Cash Rate; Financial Data) — permissive, not blocking

Both sections explicitly permit use, reproduction, publication and communication to the
public "for personal or commercial use", conditional on:

1. no statement or implication that the RBA endorses the use, beyond plain attribution;
2. no unlawful use;
3. no "improper commercial exploitation" — which for a paid service expressly includes
   "charging a fee to customers for access to the Cash Rate and/or Cash Rate Materials
   without informing customers that [they] are published on this website without a fee being
   charged by the RBA".

Redistribution is permitted. Condition (3) is why every table in this dataset is registered
**AllFree** rather than `PartBdpro` — an all-free dataset cannot engage the paid-access
condition at all.

## Verdict

Onboard as **CC BY 4.0**, restricted to RBA-sourced series, all tables AllFree.
The exclusion list is generated from the data and recorded in
`code/excluded_series.csv`, so the filter is reproducible and reviewable.

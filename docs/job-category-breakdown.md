# Job category filter breakdown: jobs and human picks

For each job (with its **job category**), every human-selected candidate who was excluded because of the job_category filter is listed with their **desired job categories** from the candidate profile. The pipeline only scores candidates whose `desired_job_categories` contain the job’s `job_category` (case-insensitive).

**How “Match?” is computed:** “Match? Yes” means the job’s category string equals (after lowercasing) one of the candidate’s desired categories. The pipeline does **exact string match** on the full `job_category` value. So:
- Multi-value job categories (e.g. `Designer, Product Designer`) are compared as a single string; a candidate with separate “Designer” and “Product Designer” gets **No** because `"designer, product designer"` ≠ `"designer"` or `"product designer"`.
- Typos (e.g. “Product Markter” on profile vs “Marketing” on job) cause **No**.
- Taxonomy differences (e.g. job “Compliance” vs candidate “Legal”) cause **No**.

---

## Rust Developer @ Job Board *(technical; from separate run)*

- **Job category:** `Rust Developer`
- **Job ATS record id:** `recjDlNrpJfRRg7b4`
- **Human picks excluded by job_category:** 24

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Noah Haufer | `rec1fUhv3K65gPUd6` | ["AI Engineer", "Full-Stack Developer", "Product Manager", "Operations"] | No |
| Shubham Singh | `rec6vPJF2Mr0GtUmC` | ["Protocol Engineer", "Backend Developer"] | No |
| Nino Borović | `rec7IaGUfodL2zlHA` | ["Backend Developer", "Full-Stack Developer"] | No |
| Prince Kumar Yadav | `rec84kgjxVZnGlqog` | ["Backend Developer", "Full-Stack Developer"] | No |
| Julien Sie | `rec8vBv0K5DvaRWZU` | ["Backend Developer", "Full-Stack Developer", "DevOps", "Project Manager"] | No |
| Farrukh Raza Zaidi | `recAlxbDWEIFyhfZG` | ["Backend Developer"] | No |
| Aman Hashim Jemal | `recAwzmiTD2ToeyiS` | ["Backend Developer", "DevRel"] | No |
| Ernest Abah | `recFpVlO3bzR3b0Q3` | ["Backend Developer", "DevOps"] | No |
| Dmitrii Shlagov | `recOzj2UJ8qZi4jYp` | ["Backend Developer"] | No |
| Chris Kim | `recRuHhn3xgjBAn3i` | ["Backend Developer", "DevRel"] | No |
| Charles Grigny | `recU1JJarmDxBg24P` | ["DevOps", "Full-Stack Developer", "Backend Developer"] | No |
| Artem Tenkov | `recXYawS8N99fk38O` | ["Backend Developer", "Full-Stack Developer"] | No |
| Zubayr Khalid | `recdPaOahvtmyWC0k` | ["Full-Stack Developer", "Research"] | No |
| Facundo La Rocca | `receJlmaR1bFVFyEH` | ["Backend Developer", "AI Engineer"] | No |
| Vitor Kretiska Medeiros | `recfZDxBIprjpuCj3` | ["Full-Stack Developer", "Backend Developer"] | No |
| German Odilov | `recgjO1gK7AG4tjYF` | ["Backend Developer", "Protocol Engineer"] | No |
| Nakshatra Nahar | `recjg1Qo0ZjMvA1VP` | ["Protocol Engineer", "AI Engineer", "Backend Developer", "Full-Stack Developer"…] | No |
| Anton Piniaz | `recmt9RaxECXhiGAI` | ["Backend Developer", "Full-Stack Developer"] | No |
| Mateusz Zając | `recqLcKm66yo0OyUX` | ["Backend Developer"] | No |
| Goran Gutovic | `recua231I1ZuKpp54` | ["Full-Stack Developer", "Backend Developer"] | No |
| Bhargav Veepuri | `recw5cmSszc2YY9kk` | ["AI Engineer", "Backend Developer", "Full-Stack Developer", "Protocol Engineer"…] | No |
| Prince Kumar Yadav | `recymzcZ4OgF7WQqL` | ["Full-Stack Developer", "Protocol Engineer"] | No |
| Islam Bekbuzarov | `reczRZPU3iNfj0qDo` | ["Backend Developer"] | No |
| Sohail Ghafoor | `reczRxRPapGh4FRn8` | ["Backend Developer"] | No |

*Rust Developer is a specific role; candidates have “Backend Developer”, “Protocol Engineer”, etc., but no one has “Rust Developer” in desired categories — taxonomy gap.*

---

## Money Laundering Reporting Officer (MLRO) @ Altitude

- **Job category:** `Compliance`
- **Job ATS record id:** `rec5yo8rhzVORjBDi`
- **Human picks excluded by job_category:** 17

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Albert Akpan | `rec1sQEYXhFnBxVHl` | ["Legal"] | No |
| Dimitrij Gede | `recADqdlbG8iLM5OR` | ["Operations", "Legal"] | No |
| Madalena Catarino | `recBZQoSmZL8vqMgX` | ["Legal"] | No |
| Christopher Guerra | `recEpgQ3rptaktgN0` | ["Legal"] | No |
| Anais Jeridi | `recJLufPYCIpGP4n1` | ["Legal"] | No |
| ARTUR ZYGARLICKI | `recJnX0Six1mcotw2` | ["Legal"] | No |
| Evelyn Retkofsky | `recK9LQfPAoF455Fy` | ["Legal"] | No |
| Sandor Hasznyuk | `recLiRbH2jmW8RtGl` | ["Legal"] | No |
| Maxim Kon | `recPGl5E5clSLPDbV` | ["Legal", "Product Manager", "Growth", "Project Manager"] | No |
| Daragh Nolan | `recRAIejiQYQ6bLko` | ["Legal"] | No |
| Antonio Balistreri | `recRfKNfLJvDjcBRJ` | ["Legal"] | No |
| Aurelie PAGON | `recSKMrcadqIbOxJd` | ["Legal", "Operations"] | No |
| Marcos Figueroa | `recSRDWtPIwdiDwcA` | ["Legal"] | No |
| Theodore MORITA | `recXsTBitIoer2rs8` | ["Legal"] | No |
| Olga Thompson | `recaCEeBdZinJT2oT` | ["Legal", "Operations"] | No |
| Eoin Kearns | `recnNJsD0sCRINMle` | ["Legal"] | No |
| Beata Wiśnicka-Zawierucha | `reczJzt6P3wyAhFzK` | ["Operations"] | No |

## Lead, Validator & Staking Growth (Solana) @ Raiku

- **Job category:** `Growth`
- **Job ATS record id:** `recsi4mTmg3Cfhzee`
- **Human picks excluded by job_category:** 5

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Chris McNicholas | `rec5n6NFLjuasxR6A` | ["Twitter Intern", "Community & Marketing", "Business Development", "Growth", "P… | Yes |
| Bayo Akins | `rec9AWcZR8ERmkf97` | ["Business Development"] | No |
| Gianluca Diciocia | `recN2BBdZPMVZuNdq` | ["Business Development", "Product Manager", "Operations", "Public Relations"] | No |
| Shirly Valge | `recotNhbIerU4CSUZ` | ["Business Development", "Growth", "Operations", "Project Manager", "Public Rela… | Yes |
| Ade Molajo | `recyFdjpTPfuGXMuA` | ["Growth"] | Yes |

## Business Development Director @ BuidlPad

- **Job category:** `Business Development`
- **Job ATS record id:** `rec1IwyAeeHUQitsw`
- **Human picks excluded by job_category:** 6

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Arisa Chelsea Ueno | `recC6AbcBF5zGgj4g` | ["Growth", "Community & Marketing"] | No |
| Jeremy Osborne | `recTNDA8eAhsG502p` | ["Business Development", "AI Engineer"] | Yes |
| William Croisettier | `recVTx2O6LE8VT84O` | ["Business Development", "Growth", "Operations"] | Yes |
| Chris Orza | `recVXyDAY0AQd1zH9` | ["Business Development"] | Yes |
| Karan Rajpal | `recp2AwF5i98u3VDs` | ["Business Development", "Research", "Growth"] | Yes |
| John Goldschmidt | `recxpnKrbAqIds04f` | ["Business Development", "Growth"] | Yes |

## Product Marketing Manager @ Squads

- **Job category:** `Growth, Marketing, Content Writer`
- **Job ATS record id:** `rece7iAGuBTfqpoJc`
- **Human picks excluded by job_category:** 1

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Aixa Rizzo | `recOud0VS958Rr86F` | ["Product Markter", "Community & Marketing"] | No |

## Growth Analyst @ Radarblock

- **Job category:** `Growth`
- **Job ATS record id:** `recIqBsuF33YrIrMX`
- **Human picks excluded by job_category:** 9

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Rahul Singh | `rec5MG1anevb86gip` | ["Frontend Developer", "Full-Stack Developer", "Protocol Engineer", "Backend Dev… | No |
| Abhz (Abhishek) S | `recJv2MefNPtSmKnh` | ["Growth", "Community & Marketing"] | Yes |
| Suhail Lone | `recOGDVjgALuhuyAk` | ["Growth", "Product Markter"] | Yes |
| Rishabh Sweet | `recSDUzWUlIVgLCnv` | ["Business Development", "Community & Marketing"] | No |
| Shreyash Shitanshu | `recVns65z4u0qF1Zl` | ["Twitter Intern", "Growth", "Community & Marketing", "Business Development"] | Yes |
| Sneha Yadav | `recZan9OZT3nttZM8` | ["Business Development", "Community & Marketing"] | No |
| Hrushi Bytes | `reca1cyalLS3REgFO` | ["Business Development", "Community & Marketing"] | No |
| Prratham Kamat | `reciMINSwDIdglVGb` | ["Business Development", "Community & Marketing"] | No |
| Rahul Singh | `rectOXBc0vBjrnPdO` | ["Community & Marketing", "Business Development"] | No |

## Compliance Operations Lead @ Altitude

- **Job category:** `Compliance`
- **Job ATS record id:** `recpwjff4QeibeGha`
- **Human picks excluded by job_category:** 7

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Jessica Ju | `rec5k0suZWPtWcTot` | ["Legal", "Operations"] | No |
| Mathieu Ladier | `rec9aKO9CHiOQtJIB` | ["Legal"] | No |
| Kia Bracy | `recEQCS5RlSWCsXiC` | ["Legal", "Operations", "Security"] | No |
| Temitope Adegun | `recQwU2MKfHpsJF7N` | ["Operations", "Legal"] | No |
| Zeeshan Quadar | `recUMAdHPF3yamSEA` | ["Legal"] | No |
| Robert L. Williams III | `recmisO2WArSe5lc6` | ["Business Development", "Customer Support"] | No |
| Temitope Lawal | `recwEspRWFmpmp1nY` | ["Customer Support", "Community & Marketing"] | No |

## Business Development Lead @ Radarblock

- **Job category:** `Business Development`
- **Job ATS record id:** `recch29d7MaREcrvB`
- **Human picks excluded by job_category:** 13

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Chirag Baid | `rec1AAR1OkwaEECdD` | ["Community & Marketing", "Twitter Intern", "Business Development"] | Yes |
| Anastasia | `rec9JKOERdEFg6ErW` | ["Business Development", "Operations"] | Yes |
| Paul Beynier | `recH0UkCOQlNZZQmi` | ["Business Development", "Community & Marketing", "Growth", "Product Markter", "… | Yes |
| Maurice Andrew | `recMEiskqNzxEeU9G` | ["Community & Marketing", "Business Development"] | Yes |
| Charlie Scott | `recNXM2zkvgPRyq1Z` | ["Growth", "Account Executive"] | No |
| Ambesh Shukla (Ash) | `recOBdjJOuIR0fjoj` | ["Business Development", "Growth"] | Yes |
| Simran Kaur | `recQJLI3pBjdDSnnj` | ["Growth", "Business Development"] | Yes |
| Mariana Ramalho Coimbra Rodrigues | `recQXEW0WGYWT4xUS` | ["Business Development", "Account Executive"] | Yes |
| Hrushi Bytes | `reca1cyalLS3REgFO` | ["Business Development", "Community & Marketing"] | Yes |
| Dev Patel | `rechukMtrlDf0GbCV` | ["Business Development", "Account Executive"] | Yes |
| Himalay Thakkar | `recspOyN22kKW2MkS` | ["Business Development", "Community & Marketing", "Customer Support", "Growth", … | Yes |
| Shivangi Pandey | `recvWpt8JYwCa5k1q` | ["Business Development", "Customer Support"] | Yes |
| Nhat Minh Pham | `recw7fm3QCZ0knlX8` | ["Business Development", "Growth"] | Yes |

## Operations Manager @ VeryAI

- **Job category:** `Operations`
- **Job ATS record id:** `rec2bjCVT0rRh0Bia`
- **Human picks excluded by job_category:** 8

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Zach Hake | `recGSQbCZeMa4wlrU` | ["Community & Marketing", "Customer Support", "Business Development", "Designer"… | Yes |
| Haley Cromer | `recSWHSbywrQ7Vbd8` | ["Growth", "DevRel"] | No |
| Jake Feigs | `recZAlzO7XPDZGG8g` | ["Business Development", "Customer Support", "Growth", "Operations", "Project Ma… | Yes |
| Eric Godwin | `recgsXW7cK6qO3tGh` | ["Business Development", "Growth"] | No |
| Kyla Ollinger | `reck0MDoX9TT5PqzH` | ["Operations"] | Yes |
| Maria Arriola | `recsBhehHwrubrquU` | ["Project Manager", "Product Manager"] | No |
| Tate Hutchinson | `recurgKYYuYU5VLEq` | ["Growth", "Business Development"] | No |
| Jack Geller | `recziZgX3T9SOsyG0` | ["Project Manager", "Product Markter", "Operations"] | Yes |

## Product Designer @ Buidlpad

- **Job category:** `Designer, Product Designer`
- **Job ATS record id:** `recnuhHToY0I7s8wy`
- **Human picks excluded by job_category:** 4

| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |
|------------------|------------------------|------------------------|--------|
| Yi Jie Shen | `rec0S2LHi5cgtlDmM` | ["Designer", "Product Designer"] | No |
| Jack Wong Man Wei | `recDlDQ9iPdfF30v3` | (none) | No |
| Yuxin Huang | `reciuj3T4egHJq53Q` | ["Designer"] | No |
| CHAO PEI RU | `rectkRvYFZyMi5yJv` | ["Product Designer"] | No |

from refresh.administration import refresh_administration
from refresh.bloods import refresh_bloods
from refresh.demographics import refresh_demographics
from refresh.diagnoses import refresh_diagnosis
from refresh.emergency import refresh_emergency
from refresh.episodes import refresh_episodes
from refresh.icu_admissions import refresh_icu_admissions
from refresh.icu_medication import refresh_icu_medication
from refresh.icu_organsupport import refresh_icu_organsupport
from refresh.icu_procedures import refresh_icu_procedures
from refresh.microbiology import refresh_microbiology
from refresh.observations import refresh_observations
from refresh.orders import refresh_orders
from refresh.prescribing import refresh_prescribing
from refresh.procedures import refresh_procedures
from refresh.transfers import refresh_transfer
from refresh.virology import refresh_virology

refresh_demographics()
refresh_bloods()
refresh_virology()
refresh_orders()
refresh_observations()
refresh_administration()
refresh_prescribing()
refresh_microbiology()
refresh_transfer()
refresh_procedures()
refresh_diagnosis()
refresh_episodes()
refresh_emergency()
refresh_icu_admissions()
refresh_icu_organsupport()
refresh_icu_medication()
refresh_icu_procedures()


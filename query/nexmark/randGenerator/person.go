package randGenerator

import (
	"bytes"
	"fmt"
	"math/rand"

	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/models"
)

const (
	// Number of yet-to-be-created people and auction ids allowed
	PERSON_ID_LEAD int64 = 10
)

var (
	// Small number of states/cities to keep the example queries simple
	US_STATES []string = []string{
		"AZ", "CA", "ID", "OR", "WA", "WY",
	}
	US_CITIES []string = []string{
		"Phoenix", "Los Angeles", "San Francisco",
		"Boise", "Portland", "Bend", "Redmond",
		"Seattle", "Kent", "Cheyenne",
	}

	FIRST_NAMES []string = []string{
		"Peter", "Paul", "Luke", "John", "Saul",
		"Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
	}
	LAST_NAMES []string = []string{
		"Shultz", "Abrams", "Spencer", "White",
		"Bartels", "Walton", "Smith", "Jones", "Noris",
	}
)

// Return a random person with next avaliable id
func NextPerson(
	nextEventID int64,
	random *rand.Rand,
	timestamp int64,
	cfg *config.NexmarkSourceConfig,
) *models.PersonEvent {
	person := &models.PersonEvent{
		V1: lastBase0PersonID(nextEventID, cfg) + 1000,
		V2: nextPersonName(random),
		V3: nextEmail(random),
		V4: nextCreditCard(random),
		V5: nextCity(random),
		V6: nextState(random),
		V7: timestamp,
	}
	return person
}

// Return a random person id (base 0) from last
// cfg.NumActivePeople active personid
func nextBase0PersonID(
	eventID int64,
	random *rand.Rand,
	cfg *config.NexmarkSourceConfig,
) int64 {
	// Choose a random auction that is still likely active.
	maxPersonID := lastBase0PersonID(eventID, cfg)
	minPersonID := max(
		maxPersonID-int64(cfg.NumActivePeople),
		0,
	)
	return minPersonID + nextLong(random, maxPersonID-minPersonID+1)
}

// Return the last valid person id (ignoring FIRST_PERSON_ID).
// will  be the current person id if due to generate a person.
func lastBase0PersonID(eventID int64, cfg *config.NexmarkSourceConfig) int64 {

	totalProportion := cfg.PersonProportion + cfg.AuctionProportion + cfg.BidProportion
	epoch := eventID / int64(totalProportion)
	offset := eventID % int64(totalProportion)
	if offset >= int64(cfg.PersonProportion) {
		// About to generate an auction or bid.
		// Go back to the last person generated in this epoch.
		offset = int64(cfg.PersonProportion) - 1
	}

	return epoch*int64(cfg.PersonProportion) + offset
}

// Return a random us state
func nextState(random *rand.Rand) string {
	return US_STATES[random.Intn(len(US_STATES))]
}

// Return a random us city
func nextCity(random *rand.Rand) string {
	return US_CITIES[random.Intn(len(US_CITIES))]
}

// Return a random person name
func nextPersonName(random *rand.Rand) string {
	return FIRST_NAMES[random.Intn(len(FIRST_NAMES))] + " " + LAST_NAMES[random.Intn(len(LAST_NAMES))]
}

// Return a random email
func nextEmail(random *rand.Rand) string {
	return nextString(random, 7) + "@" + nextString(random, 5) + ".com"
}

// Return a random credit card number
func nextCreditCard(random *rand.Rand) string {
	var buffer bytes.Buffer

	for i := 0; i < 4; i++ {
		if i > 0 {
			buffer.WriteString("-")
		}
		buffer.WriteString(fmt.Sprintf("%04d", random.Intn(10000)))
	}

	return buffer.String()
}
